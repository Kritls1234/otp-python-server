"""
Yopmail HTML Reader + 2Captcha - Smart Content Extraction
- ดึงเฉพาะเนื้อหาอีเมลจริง ตัด Yopmail UI + FW header
- หาเลข OTP โดยข้ามปี ค.ศ./พ.ศ. และเลขเบอร์โทร
"""
import os, re, time, asyncio, logging, urllib.parse, html as html_mod
from typing import Any, Dict, Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from playwright.async_api import async_playwright, Browser

try:
    from twocaptcha import TwoCaptcha
    HAS_2CAPTCHA = True
except ImportError:
    HAS_2CAPTCHA = False

logger = logging.getLogger("otp-server.yopmail")

YOPMAIL_MAX_SESSIONS  = int(os.getenv("YOPMAIL_MAX_SESSIONS", "4"))
YOPMAIL_NAV_TIMEOUT   = int(os.getenv("YOPMAIL_NAV_TIMEOUT_MS", "30000"))
YOPMAIL_SESSION_TOKEN = os.getenv("YOPMAIL_SESSION_TOKEN", "kritticool_yop_7h2x9k4m")
YOPMAIL_CORS_ORIGINS  = os.getenv("YOPMAIL_CORS_ORIGINS", "*").split(",")
TWOCAPTCHA_API_KEY    = os.getenv("TWOCAPTCHA_API_KEY", "")
USER_AGENT = "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36"

_browser: Optional[Browser] = None
_pw = None
_semaphore: Optional[asyncio.Semaphore] = None
_launch_lock = asyncio.Lock()
_solver = None


class YopFetchReq(BaseModel):
    email: str
    token: str = ""


async def _ensure_browser():
    global _browser, _pw
    if _browser is not None:
        return True
    async with _launch_lock:
        if _browser is not None:
            return True
        try:
            _pw = await async_playwright().start()
            _browser = await _pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            logger.info("[yopmail] chromium launched")
            return True
        except Exception:
            logger.exception("[yopmail] launch failed")
            return False


async def yopmail_startup():
    global _semaphore, _solver
    _semaphore = asyncio.Semaphore(YOPMAIL_MAX_SESSIONS)
    if HAS_2CAPTCHA and TWOCAPTCHA_API_KEY:
        try:
            _solver = TwoCaptcha(TWOCAPTCHA_API_KEY, defaultTimeout=180, pollingInterval=5)
            logger.info("[yopmail] 2Captcha solver ready")
        except Exception:
            logger.exception("[yopmail] 2Captcha init failed")
            _solver = None
    else:
        logger.warning("[yopmail] 2Captcha disabled")
    logger.info("[yopmail] ready")


async def yopmail_shutdown():
    global _browser, _pw
    try:
        if _browser:
            await _browser.close()
        if _pw:
            await _pw.stop()
    except Exception: pass


def _check_tok(t):
    return (not YOPMAIL_SESSION_TOKEN) or t == YOPMAIL_SESSION_TOKEN


async def _solve_hcaptcha(sitekey, page_url):
    if not _solver: return None
    def _solve():
        try:
            return _solver.hcaptcha(sitekey=sitekey, url=page_url).get("code")
        except Exception:
            logger.exception("[yopmail] hCaptcha solve failed")
            return None
    return await asyncio.to_thread(_solve)


async def _solve_recaptcha(sitekey, page_url):
    if not _solver: return None
    def _solve():
        try:
            return _solver.recaptcha(sitekey=sitekey, url=page_url).get("code")
        except Exception:
            logger.exception("[yopmail] reCAPTCHA solve failed")
            return None
    return await asyncio.to_thread(_solve)


async def _detect_and_solve_captcha(page):
    try:
        hcap = await page.query_selector("iframe[src*='hcaptcha.com']")
        if hcap:
            sitekey = None
            try:
                src = await hcap.get_attribute("src")
                if src:
                    m = re.search(r"sitekey=([a-f0-9\-]+)", src)
                    if m: sitekey = m.group(1)
            except Exception: pass
            if not sitekey:
                try:
                    el = await page.query_selector("[data-sitekey]")
                    if el: sitekey = await el.get_attribute("data-sitekey")
                except Exception: pass
            if not sitekey: return False
            token = await _solve_hcaptcha(sitekey, page.url)
            if not token: return False
            await page.evaluate(
                """(token) => {
                    const ta = document.querySelectorAll('textarea[name="h-captcha-response"], textarea[name="g-recaptcha-response"]');
                    ta.forEach(t => { t.value = token; t.style.display = 'block'; });
                    try {
                        const widgets = document.querySelectorAll('.h-captcha, [data-sitekey]');
                        widgets.forEach(w => {
                            const cb = w.getAttribute('data-callback');
                            if (cb && typeof window[cb] === 'function') window[cb](token);
                        });
                    } catch(e) {}
                }""", token)
            await page.wait_for_timeout(800)
            return True

        recap = await page.query_selector("iframe[src*='recaptcha']")
        if recap:
            sitekey = None
            try:
                el = await page.query_selector("[data-sitekey]")
                if el: sitekey = await el.get_attribute("data-sitekey")
            except Exception: pass
            if not sitekey: return False
            token = await _solve_recaptcha(sitekey, page.url)
            if not token: return False
            await page.evaluate(
                """(token) => {
                    const ta = document.querySelectorAll('textarea[name="g-recaptcha-response"]');
                    ta.forEach(t => { t.value = token; t.style.display = 'block'; });
                }""", token)
            await page.wait_for_timeout(800)
            return True
        return False
    except Exception:
        return False


def _strip_fw_header(html: str) -> str:
    """ตัด FW header block ออก เหลือเฉพาะเนื้อหาตั้งแต่ Subject: ลงไป"""
    if not html:
        return ""

    # ลองหา anchor: ข้อความตั้งแต่ "From:" จนถึง "Subject:" คือ FW header
    # ตัดทุกอย่างก่อน "Subject:" (รวม Subject: line) ออก

    # Method 1: ตัดจาก "From:" ถึงบรรทัดถัดจาก "Subject:"
    # pattern ครอบคลุม: From: ... Sent: ... To: ... Subject: <subject>
    fw_pattern = re.compile(
        r"From\s*:.*?Subject\s*:[^\n<]*(?:</[^>]+>|\n|<br[^>]*>)",
        re.IGNORECASE | re.DOTALL,
    )
    html = fw_pattern.sub("", html)

    # Method 2: ลบ "FW: ..." ที่ขึ้นต้นเนื้อหา
    html = re.sub(r"^[\s\S]*?FW\s*:[^<\n]*(?:<br[^>]*>|\n)", "", html, flags=re.IGNORECASE)

    # Method 3: ลบ pattern email header ที่เหลือ (Sunday, May... date line)
    html = re.sub(
        r"(?:Sun|Mon|Tue|Wed|Thu|Fri|Sat)(?:day)?,\s+[A-Z][a-z]+\s+\d+,\s+\d{4}\s+\d+:\d+:\d+\s*(?:AM|PM)?[^\n<]*",
        "", html, flags=re.IGNORECASE,
    )

    # Method 4: ลบบรรทัด <email@domain> ที่เป็น sender info
    html = re.sub(r"<[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+>", "", html)

    # Method 5: ลบ "sXA3QDYhI AJwo3J0e" Yopmail random string at top
    # pattern: 10-30 ตัวอักษรผสมตัวเลขที่ดูสุ่ม + อาจตามด้วย <email>
    html = re.sub(r"^[\s\S]{0,80}?[A-Za-z][A-Za-z0-9]{8,20}\s+[A-Za-z][A-Za-z0-9]{4,20}\s*(?=<br|<p|<div|\n)", "", html, count=1)

    return html


def _clean_email_html(html: str) -> str:
    """ตัด Yopmail UI + FW header ออก เหลือเฉพาะเนื้อหาอีเมลจริง"""
    if not html:
        return ""

    # 1. ลบ script/style/iframe
    html = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<style[\s\S]*?</style>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<iframe[\s\S]*?</iframe>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<noscript[\s\S]*?</noscript>", "", html, flags=re.IGNORECASE)

    # 2. ลบ Yopmail toolbar
    html = re.sub(r"<div[^>]*id=[\"']nbmail[\"'][\s\S]*?</div>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<div[^>]*class=[\"'][^\"']*\b(?:mb|nb|opt)\b[^\"']*[\"'][\s\S]*?</div>", "", html, flags=re.IGNORECASE)

    # 3. ลบปุ่ม/input
    html = re.sub(r"<button[\s\S]*?</button>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<input[^>]*type=[\"']?(?:button|submit|checkbox|radio)[\"']?[^>]*>", "", html, flags=re.IGNORECASE)

    # 4. ตัด FW header
    html = _strip_fw_header(html)

    # 5. ลบ checkbox icons
    html = re.sub(r"[\u2610-\u2612\u25A0-\u25A1\u2B1B\u2B1C]", "", html)

    # 6. ลบ "Show pictures"
    html = re.sub(r"<a[^>]*>\s*(?:Show\s*pictures|แสดง\s*รูป)\s*</a>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"(?:Show\s*pictures|แสดง\s*รูป)", "", html, flags=re.IGNORECASE)

    # 7. ลบ on* attributes + javascript:
    html = re.sub(r"\son\w+\s*=\s*\"[^\"]*\"", "", html, flags=re.IGNORECASE)
    html = re.sub(r"\son\w+\s*=\s*'[^']*'", "", html, flags=re.IGNORECASE)
    html = re.sub(r"javascript:", "blocked:", html, flags=re.IGNORECASE)

    # 8. ลบ comments
    html = re.sub(r"<!--[\s\S]*?-->", "", html)

    # 9. ลบ empty tags ที่เกิดจากการลบ
    for _ in range(4):
        html = re.sub(r"<(div|span|p|td|tr)[^>]*>\s*</\1>", "", html, flags=re.IGNORECASE)
        html = re.sub(r"<(div|span|p|td|tr)[^>]*>(?:&nbsp;|\s)*</\1>", "", html, flags=re.IGNORECASE)

    return html.strip()


# ── Smart OTP extraction ที่ข้ามปี / เบอร์โทร ──
def _smart_extract_code(text: str) -> Optional[str]:
    """หาเลข OTP จาก text โดยข้ามปี ค.ศ./พ.ศ. และเบอร์โทร"""
    if not text:
        return None

    # ปกติ OTP จะมี keyword ใกล้ๆ → priority สูง
    priority_patterns = [
        # ยืนยันด้วยรหัสนี้: 361609
        r"ยืนยันด้วยรหัสนี้\s*[:：]?\s*([0-9]{4,8})",
        r"ป้อนรหัสนี้เพื่อ\S{0,20}\s*[:：]?\s*([0-9]{4,8})",
        r"ป้อน\s*รหัส\s*นี้\s*[:：]?\s*([0-9]{4,8})",
        r"รหัสยืนยัน\s*[:：]?\s*([0-9]{4,8})",
        r"รหัสเข้าสู่ระบบ\s*[:：]?\s*([0-9]{4,8})",
        r"verification\s*code\s*[:：]?\s*([0-9]{4,8})",
        r"sign[-\s]*in\s*code\s*[:：]?\s*([0-9]{4,8})",
        r"login\s*code\s*[:：]?\s*([0-9]{4,8})",
        r"enter\s*this\s*code\s*(?:to\s*sign\s*in)?\s*[:：]?\s*([0-9]{4,8})",
        r"your\s*code\s*[:：]?\s*([0-9]{4,8})",
    ]
    for pat in priority_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            digits = m.group(1)
            if _is_valid_otp(digits):
                return digits

    # spaced digits (3 6 1 6 0 9)
    m6 = re.search(r"(?<!\d)((?:\d\s){5}\d)(?!\s*\d)", text)
    if m6:
        digits = re.sub(r"\s+", "", m6.group(1))
        if _is_valid_otp(digits):
            return digits

    # หาเลข 6 หลัก หรือ 4 หลักทั้งหมด แล้วเลือกอันที่ valid
    candidates = re.findall(r"(?<!\d)(\d{4,8})(?!\d)", text)
    # priority: 6 หลักก่อน, ตามด้วย 8, 7, 5, 4
    priority_lens = [6, 8, 7, 5, 4]
    for plen in priority_lens:
        for c in candidates:
            if len(c) == plen and _is_valid_otp(c):
                return c

    return None


def _is_valid_otp(digits: str) -> bool:
    """ตรวจสอบว่าเลขชุดนี้น่าจะเป็น OTP จริงไหม"""
    if not digits or not digits.isdigit():
        return False
    n = len(digits)
    if n < 4 or n > 8:
        return False
    # ปี ค.ศ. 1900-2099
    if n == 4 and re.match(r"^(19|20)\d{2}$", digits):
        return False
    # ปี พ.ศ. 2400-2700 (พ.ศ. 4 หลัก)
    if n == 4 and re.match(r"^(24|25|26|27)\d{2}$", digits):
        return False
    # ปี พ.ศ. 4 หลักเฉยๆ ที่ปกติ (2400-2700)
    val = int(digits) if n <= 4 else 0
    if n == 4 and 2400 <= val <= 2799:
        return False
    # เลขซ้ำกันหมด เช่น 0000, 111111 → ไม่ใช่ OTP จริง
    if len(set(digits)) == 1:
        return False
    # เบอร์โทร 8+ หลักที่ขึ้นต้น 02, 06, 08, 09
    if n >= 8 and re.match(r"^0[2689]", digits):
        return False
    return True


async def _fetch_latest_email(shortname, code_extractor) -> Dict[str, Any]:
    ctx = await _browser.new_context(
        viewport={"width": 420, "height": 740},
        user_agent=USER_AGENT,
        locale="en-US",
    )
    try:
        page = await ctx.new_page()
        page.set_default_timeout(YOPMAIL_NAV_TIMEOUT)

        await page.goto("https://yopmail.com/en/", wait_until="domcontentloaded")
        await page.wait_for_timeout(1500)

        try:
            await page.fill("#login", shortname, timeout=8000)
            await page.wait_for_timeout(500)
        except Exception: pass

        try:
            await page.click("#refreshbut .md, #refreshbut", timeout=5000)
        except Exception:
            await page.goto(f"https://yopmail.com/en/wm?login={urllib.parse.quote(shortname)}",
                            wait_until="domcontentloaded")

        await page.wait_for_timeout(2000)

        for _ in range(2):
            solved = await _detect_and_solve_captcha(page)
            if solved:
                await page.wait_for_timeout(2500)
                try:
                    submit = await page.query_selector("button[type=submit], input[type=submit], #yes")
                    if submit:
                        await submit.click()
                        await page.wait_for_timeout(2500)
                except Exception: pass
            else:
                break

        list_frame = None
        for f in page.frames:
            try:
                if "ifinbox" in (f.name or "").lower() or "inbox" in (f.url or "").lower():
                    list_frame = f
                    break
            except Exception: continue

        first_mail_data = None
        try:
            if list_frame:
                await list_frame.wait_for_selector("button.lm, div.m", timeout=8000)
                first_mail = await list_frame.query_selector("button.lm, div.m")
                if first_mail:
                    subject = sender = date = ""
                    try:
                        el = await first_mail.query_selector(".lms")
                        if el: subject = (await el.inner_text() or "").strip()
                    except Exception: pass
                    try:
                        el = await first_mail.query_selector(".lmf")
                        if el: sender = (await el.inner_text() or "").strip()
                    except Exception: pass
                    try:
                        el = await first_mail.query_selector(".lmh")
                        if el: date = (await el.inner_text() or "").strip()
                    except Exception: pass
                    first_mail_data = {"subject": subject, "from": sender, "date": date}
                    await first_mail.click()
                    await page.wait_for_timeout(1800)
        except Exception:
            logger.exception("[yopmail] click first mail failed")

        if not first_mail_data:
            solved = await _detect_and_solve_captcha(page)
            if solved:
                await page.wait_for_timeout(2500)
                for f in page.frames:
                    try:
                        if "ifinbox" in (f.name or "").lower() or "inbox" in (f.url or "").lower():
                            list_frame = f
                            break
                    except Exception: continue
                if list_frame:
                    try:
                        await list_frame.wait_for_selector("button.lm, div.m", timeout=6000)
                        first_mail = await list_frame.query_selector("button.lm, div.m")
                        if first_mail:
                            subject = sender = date = ""
                            try:
                                el = await first_mail.query_selector(".lms")
                                if el: subject = (await el.inner_text() or "").strip()
                            except Exception: pass
                            try:
                                el = await first_mail.query_selector(".lmf")
                                if el: sender = (await el.inner_text() or "").strip()
                            except Exception: pass
                            try:
                                el = await first_mail.query_selector(".lmh")
                                if el: date = (await el.inner_text() or "").strip()
                            except Exception: pass
                            first_mail_data = {"subject": subject, "from": sender, "date": date}
                            await first_mail.click()
                            await page.wait_for_timeout(1800)
                    except Exception: pass

        if not first_mail_data:
            return {"success": False, "message": "ยังไม่มีอีเมลในกล่อง หรือระบบติด CAPTCHA", "empty": True}

        # ดึง HTML จาก ifmail frame เท่านั้น (เนื้อหาล้วน ไม่มี toolbar)
        mail_html = ""
        mail_text = ""
        for f in page.frames:
            try:
                fname = (f.name or "").lower()
                furl = (f.url or "").lower()
                if "ifmail" in fname or "/mail?b=" in furl or "/m?b=" in furl:
                    body = await f.query_selector("body")
                    if body:
                        html_content = await body.inner_html()
                        text_content = await body.inner_text()
                        if html_content and len(html_content) > 50:
                            mail_html = html_content
                            mail_text = text_content or ""
                            break
            except Exception: continue

        if not mail_html:
            best_len = 0
            for f in page.frames:
                try:
                    if "ifinbox" in (f.name or "").lower():
                        continue
                    body = await f.query_selector("body")
                    if body:
                        text_content = await body.inner_text() or ""
                        if len(text_content) > best_len:
                            best_len = len(text_content)
                            mail_html = await body.inner_html()
                            mail_text = text_content
                except Exception: continue

        if not mail_html:
            return {"success": False, "message": "อ่านเนื้อหาอีเมลไม่สำเร็จ"}

        # ── ใช้ subject เป็น hint ในการหา code ──
        subject = first_mail_data.get("subject", "")

        # ── ดึง code ใช้ smart extractor ก่อน fallback ไป code_extractor เดิม ──
        code = None
        try:
            search_text = mail_text or html_mod.unescape(re.sub(r"<[^>]+>", " ", mail_html))
            # ตัด FW header / mail header ออกจาก search text ก่อน
            search_text = _strip_text_header(search_text)
            code = _smart_extract_code(search_text)
            if not code:
                # fallback ใช้ extractor เดิม
                code = code_extractor(search_text)
        except Exception:
            logger.exception("[yopmail] code extract error")

        # ── Clean HTML ──
        clean_html = _clean_email_html(mail_html)

        return {
            "success": True,
            "subject": subject,
            "from": first_mail_data.get("from", ""),
            "date": first_mail_data.get("date", ""),
            "html": clean_html,
            "code": code or "",
        }
    finally:
        try:
            await ctx.close()
        except Exception: pass


def _strip_text_header(text: str) -> str:
    """ตัด FW header ใน plain text ออก เหลือเฉพาะเนื้อหา"""
    if not text:
        return ""
    # ตัดทุกอย่างก่อน "Subject:" บรรทัดสุดท้าย (ถ้ามี)
    lines = text.split("\n")
    cut_idx = -1
    for i, line in enumerate(lines):
        if re.match(r"^\s*Subject\s*:", line, re.IGNORECASE):
            cut_idx = i
    if cut_idx >= 0 and cut_idx < len(lines) - 1:
        return "\n".join(lines[cut_idx + 1:])
    return text


def register_yopmail_routes(app: FastAPI, code_extractor):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=YOPMAIL_CORS_ORIGINS,
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )

    @app.post("/yopmail/fetch")
    async def yopmail_fetch(data: YopFetchReq):
        if not _check_tok(data.token):
            return {"success": False, "message": "ไม่ได้รับอนุญาต"}
        if not await _ensure_browser():
            return {"success": False, "message": "ระบบบราวเซอร์ยังไม่พร้อม"}

        email = str(data.email or "").replace(" ", "").lower().strip()
        shortname = email.split("@")[0] if "@" in email else email
        if not shortname:
            return {"success": False, "message": "รูปแบบอีเมลไม่ถูกต้อง"}

        if _semaphore.locked() and _semaphore._value == 0:
            return {"success": False, "message": "ระบบกำลังใช้งานเต็ม กรุณารอสักครู่"}

        async with _semaphore:
            try:
                t0 = time.time()
                result = await _fetch_latest_email(shortname, code_extractor)
                logger.info("[yopmail] fetch %s in %.2fs success=%s code=%s",
                            shortname, time.time() - t0, result.get("success"), result.get("code", "")[:3] + "***" if result.get("code") else "-")
                return result
            except Exception:
                logger.exception("[yopmail] fetch failed")
                return {"success": False, "message": "อ่านกล่องอีเมลไม่สำเร็จ กรุณาลองใหม่"}

"""
Yopmail HTML Reader + 2Captcha — Clean Content Only
- ดึงเฉพาะเนื้อหาอีเมล ตัดปุ่ม Yopmail / FW header / Show pictures ออก
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
        logger.warning("[yopmail] 2Captcha disabled (no API key or library)")
    logger.info("[yopmail] ready")


async def yopmail_shutdown():
    global _browser, _pw
    try:
        if _browser:
            await _browser.close()
        if _pw:
            await _pw.stop()
    except Exception:
        pass


def _check_tok(t):
    return (not YOPMAIL_SESSION_TOKEN) or t == YOPMAIL_SESSION_TOKEN


async def _solve_hcaptcha(sitekey: str, page_url: str) -> Optional[str]:
    if not _solver:
        return None
    def _solve():
        try:
            return _solver.hcaptcha(sitekey=sitekey, url=page_url).get("code")
        except Exception:
            logger.exception("[yopmail] hCaptcha solve failed")
            return None
    return await asyncio.to_thread(_solve)


async def _solve_recaptcha(sitekey: str, page_url: str) -> Optional[str]:
    if not _solver:
        return None
    def _solve():
        try:
            return _solver.recaptcha(sitekey=sitekey, url=page_url).get("code")
        except Exception:
            logger.exception("[yopmail] reCAPTCHA solve failed")
            return None
    return await asyncio.to_thread(_solve)


async def _detect_and_solve_captcha(page) -> bool:
    try:
        hcap = await page.query_selector("iframe[src*='hcaptcha.com']")
        if hcap:
            logger.info("[yopmail] hCaptcha detected")
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
            logger.info("[yopmail] reCAPTCHA detected")
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


def _clean_email_html(html: str) -> str:
    """ตัด Yopmail UI ออก เหลือเฉพาะเนื้อหาอีเมลจริง"""
    if not html:
        return ""

    # ลบ script/style/iframe
    html = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<style[\s\S]*?</style>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<iframe[\s\S]*?</iframe>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<noscript[\s\S]*?</noscript>", "", html, flags=re.IGNORECASE)

    # ลบ Yopmail toolbar (div#nbmail, div.mb, button bar)
    html = re.sub(r"<div[^>]*id=[\"']nbmail[\"'][\s\S]*?</div>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<div[^>]*class=[\"'][^\"']*\b(?:mb|nb|opt)\b[^\"']*[\"'][\s\S]*?</div>", "", html, flags=re.IGNORECASE)

    # ลบปุ่มทั้งหมด (Yopmail ใช้ <button> สำหรับ Reply/Forward/Delete ฯลฯ)
    html = re.sub(r"<button[\s\S]*?</button>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<input[^>]*type=[\"']?(?:button|submit|checkbox|radio)[\"']?[^>]*>", "", html, flags=re.IGNORECASE)

    # ลบ FW header / mail-info block (เก็บเฉพาะเนื้อหา)
    # Yopmail วาง header ใน div#mailmillieu, div.mailout, div#mail (ขึ้นกับ version)
    # เราจะตัดเฉพาะถ้าเจอ pattern ที่บ่งบอกว่าเป็น header
    html = re.sub(r"<div[^>]*\b(?:From|Sent|To|Subject)\s*:.*?</div>", "", html, flags=re.IGNORECASE | re.DOTALL)

    # ลบ checkbox icons (ตัว □ ที่เห็นในรูป)
    html = re.sub(r"[\u2610-\u2612\u25A0-\u25A1\u2B1B\u2B1C]", "", html)

    # ลบ "Show pictures" link
    html = re.sub(r"<a[^>]*>\s*(?:Show\s*pictures|แสดง\s*รูป)\s*</a>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"(?:Show\s*pictures|แสดง\s*รูป)", "", html, flags=re.IGNORECASE)

    # ลบ on* attributes
    html = re.sub(r"\son\w+\s*=\s*\"[^\"]*\"", "", html, flags=re.IGNORECASE)
    html = re.sub(r"\son\w+\s*=\s*'[^']*'", "", html, flags=re.IGNORECASE)
    html = re.sub(r"javascript:", "blocked:", html, flags=re.IGNORECASE)

    # ลบ class/id attributes ที่อ้างถึง Yopmail (ลด CSS conflict)
    # html = re.sub(r"\sclass=[\"'][^\"']*[\"']", "", html)  # อย่าลบ class เพราะอีเมลจริงอาจใช้

    # ลบ comments
    html = re.sub(r"<!--[\s\S]*?-->", "", html)

    # ลบ empty divs/spans ที่เกิดจากการลบ
    for _ in range(3):
        html = re.sub(r"<(div|span|p)[^>]*>\s*</\1>", "", html, flags=re.IGNORECASE)

    return html.strip()


async def _fetch_latest_email(shortname: str, code_extractor) -> Dict[str, Any]:
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

        # ── ดึง HTML เฉพาะจาก iframe ของอีเมล (ifmail) ──
        # อย่าใช้ body ของ frame หลัก เพราะจะติด toolbar ของ Yopmail
        mail_html = ""
        mail_text = ""
        for f in page.frames:
            try:
                fname = (f.name or "").lower()
                furl = (f.url or "").lower()
                # ifmail = frame ของเนื้อหาอีเมลล้วนๆ (ไม่มี toolbar)
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

        # fallback: เลือก frame ที่ text ยาวที่สุด (น่าจะเป็นเนื้อหาอีเมล)
        if not mail_html:
            best_len = 0
            for f in page.frames:
                try:
                    # ข้าม inbox list frame
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

        # ── ทำความสะอาด HTML ──
        clean_html = _clean_email_html(mail_html)

        # ── ดึง code จาก text ──
        code = None
        try:
            search_text = mail_text or html_mod.unescape(re.sub(r"<[^>]+>", " ", mail_html))
            code = code_extractor(search_text)
        except Exception: pass

        return {
            "success": True,
            "subject": first_mail_data.get("subject", ""),
            "from": first_mail_data.get("from", ""),
            "date": first_mail_data.get("date", ""),
            "html": clean_html,
            "code": code or "",
        }
    finally:
        try:
            await ctx.close()
        except Exception: pass


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
                logger.info("[yopmail] fetch %s in %.2fs success=%s",
                            shortname, time.time() - t0, result.get("success"))
                return result
            except Exception:
                logger.exception("[yopmail] fetch failed")
                return {"success": False, "message": "อ่านกล่องอีเมลไม่สำเร็จ กรุณาลองใหม่"}

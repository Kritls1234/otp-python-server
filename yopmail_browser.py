"""
Yopmail HTML Reader — ดึง HTML จริงของอีเมลล่าสุด (ไม่ใช้ screenshot)
"""
import os, re, time, base64, asyncio, logging, urllib.parse, html as html_mod
from typing import Any, Dict, Optional, List
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from playwright.async_api import async_playwright, Browser

logger = logging.getLogger("otp-server.yopmail")

YOPMAIL_MAX_SESSIONS  = int(os.getenv("YOPMAIL_MAX_SESSIONS", "8"))
YOPMAIL_NAV_TIMEOUT   = int(os.getenv("YOPMAIL_NAV_TIMEOUT_MS", "20000"))
YOPMAIL_SESSION_TOKEN = os.getenv("YOPMAIL_SESSION_TOKEN", "kritticool_yop_7h2x9k4m")
YOPMAIL_CORS_ORIGINS  = os.getenv("YOPMAIL_CORS_ORIGINS", "*").split(",")
USER_AGENT = "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36"

_browser: Optional[Browser] = None
_pw = None
_semaphore: Optional[asyncio.Semaphore] = None
_launch_lock = asyncio.Lock()


class YopFetchReq(BaseModel):
    email: str
    token: str = ""


async def _ensure_browser():
    """Lazy launch chromium on first request"""
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
                args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu","--disable-blink-features=AutomationControlled"]
            )
            logger.info("[yopmail] chromium launched")
            return True
        except Exception:
            logger.exception("[yopmail] launch failed")
            return False


async def yopmail_startup():
    global _semaphore
    _semaphore = asyncio.Semaphore(YOPMAIL_MAX_SESSIONS)
    logger.info("[yopmail] ready (lazy launch on first request)")


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


async def _fetch_latest_email(shortname: str, code_extractor) -> Dict[str, Any]:
    """เปิด Yopmail → คลิกอีเมลล่าสุด → ดึง HTML"""
    ctx = await _browser.new_context(
        viewport={"width": 420, "height": 740},
        user_agent=USER_AGENT,
        locale="en-US",
    )
    try:
        page = await ctx.new_page()
        page.set_default_timeout(YOPMAIL_NAV_TIMEOUT)

        # เปิด inbox
        inbox_url = f"https://yopmail.com/en/wm?login={urllib.parse.quote(shortname)}"
        await page.goto(inbox_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1500)

        # หา iframe ของ inbox list (Yopmail ใช้ frame ชื่อ "ifinboxlist")
        list_frame = None
        for f in page.frames:
            try:
                if "inbox" in (f.url or "").lower() or "ifinbox" in (f.name or "").lower():
                    list_frame = f
                    break
            except Exception:
                continue

        # ลอง click อีเมลแรก
        first_mail_data = None
        try:
            if list_frame:
                # อีเมลใน Yopmail อยู่ใน div.m หรือ button.lm
                await list_frame.wait_for_selector("button.lm, div.m", timeout=5000)
                first_mail = await list_frame.query_selector("button.lm, div.m")
                if first_mail:
                    # ดึง metadata จาก list
                    subject = ""
                    sender = ""
                    date = ""
                    try:
                        subject_el = await first_mail.query_selector(".lms")
                        if subject_el:
                            subject = (await subject_el.inner_text()).strip()
                    except Exception:
                        pass
                    try:
                        sender_el = await first_mail.query_selector(".lmf")
                        if sender_el:
                            sender = (await sender_el.inner_text()).strip()
                    except Exception:
                        pass
                    try:
                        date_el = await first_mail.query_selector(".lmh")
                        if date_el:
                            date = (await date_el.inner_text()).strip()
                    except Exception:
                        pass

                    first_mail_data = {"subject": subject, "from": sender, "date": date}
                    await first_mail.click()
                    await page.wait_for_timeout(1500)
        except Exception:
            logger.exception("[yopmail] click first mail failed")

        if not first_mail_data:
            return {"success": False, "message": "ยังไม่มีอีเมลในกล่อง", "empty": True}

        # ดึง HTML ของอีเมลจาก iframe "ifmail"
        mail_html = ""
        mail_text = ""
        for f in page.frames:
            try:
                fname = (f.name or "").lower()
                furl = (f.url or "").lower()
                if "ifmail" in fname or "/mail?" in furl or "/m?" in furl:
                    body = await f.query_selector("body")
                    if body:
                        mail_html = await body.inner_html()
                        mail_text = await body.inner_text()
                        if mail_html and len(mail_html) > 50:
                            break
            except Exception:
                continue

        # fallback: ลองจากทุก frame
        if not mail_html:
            for f in page.frames:
                try:
                    body = await f.query_selector("body")
                    if body:
                        html_content = await body.inner_html()
                        text_content = await body.inner_text()
                        if len(text_content or "") > len(mail_text or ""):
                            mail_html = html_content
                            mail_text = text_content
                except Exception:
                    continue

        if not mail_html:
            return {"success": False, "message": "อ่านเนื้อหาอีเมลไม่สำเร็จ"}

        # extract code
        code = None
        try:
            code = code_extractor(mail_text or html_mod.unescape(re.sub(r"<[^>]+>", " ", mail_html)))
        except Exception:
            pass

        return {
            "success": True,
            "subject": first_mail_data.get("subject", ""),
            "from": first_mail_data.get("from", ""),
            "date": first_mail_data.get("date", ""),
            "html": _sanitize_html(mail_html),
            "code": code or "",
        }
    finally:
        try:
            await ctx.close()
        except Exception:
            pass


def _sanitize_html(html: str) -> str:
    """ลบ script tags, on* attributes, javascript: links เพื่อความปลอดภัย"""
    if not html:
        return ""
    html = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"<style[\s\S]*?</style>", "", html, flags=re.IGNORECASE)
    html = re.sub(r"\son\w+\s*=\s*\"[^\"]*\"", "", html, flags=re.IGNORECASE)
    html = re.sub(r"\son\w+\s*=\s*'[^']*'", "", html, flags=re.IGNORECASE)
    html = re.sub(r"javascript:", "blocked:", html, flags=re.IGNORECASE)
    return html


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

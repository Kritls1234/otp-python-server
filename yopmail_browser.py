"""
Yopmail Browser Session — วางไฟล์นี้ข้างๆ main.py บน Render
แล้วเพิ่มใน main.py แค่ 4 บรรทัด (ดูคำแนะนำท้ายไฟล์)
"""
import os, re, time, base64, asyncio, logging, urllib.parse
from typing import Any, Dict, Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

logger = logging.getLogger("otp-server.yopmail")

YOPMAIL_MAX_SESSIONS     = int(os.getenv("YOPMAIL_MAX_SESSIONS", "6"))
YOPMAIL_SESSION_TTL      = int(os.getenv("YOPMAIL_SESSION_TTL", "240"))
YOPMAIL_CLEANUP_INTERVAL = int(os.getenv("YOPMAIL_CLEANUP_INTERVAL", "30"))
YOPMAIL_VIEWPORT_W       = int(os.getenv("YOPMAIL_VIEWPORT_W", "420"))
YOPMAIL_VIEWPORT_H       = int(os.getenv("YOPMAIL_VIEWPORT_H", "740"))
YOPMAIL_NAV_TIMEOUT_MS   = int(os.getenv("YOPMAIL_NAV_TIMEOUT_MS", "25000"))
YOPMAIL_SESSION_TOKEN    = os.getenv("YOPMAIL_SESSION_TOKEN", "kritticool_yop_7h2x9k4m")
YOPMAIL_CORS_ORIGINS     = os.getenv("YOPMAIL_CORS_ORIGINS", "*").split(",")
YOPMAIL_USER_AGENT = "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36"

class YopmailSession:
    __slots__ = ("id","email","shortname","context","page","created_at","last_active","lock","closed")
    def __init__(self, sid, email, shortname, context, page):
        self.id=sid; self.email=email; self.shortname=shortname
        self.context=context; self.page=page
        self.created_at=time.time(); self.last_active=time.time()
        self.lock=asyncio.Lock(); self.closed=False
    def touch(self): self.last_active=time.time()

_yop_browser: Optional[Browser] = None
_yop_pw = None
_yop_sessions: Dict[str, YopmailSession] = {}
_yop_store_lock = asyncio.Lock()
_yop_semaphore: Optional[asyncio.Semaphore] = None
_yop_cleanup_task = None

class YopStartReq(BaseModel):
    email: str; token: str = ""
class YopClickReq(BaseModel):
    token: str = ""; x: float; y: float; view_w: float = 0; view_h: float = 0
class YopActionReq(BaseModel):
    token: str = ""

async def yopmail_startup():
    global _yop_browser, _yop_pw, _yop_semaphore, _yop_cleanup_task
    _yop_semaphore = asyncio.Semaphore(YOPMAIL_MAX_SESSIONS)
    try:
        _yop_pw = await async_playwright().start()
        _yop_browser = await _yop_pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu","--disable-blink-features=AutomationControlled"])
        logger.info("[yopmail] chromium launched (max=%d)", YOPMAIL_MAX_SESSIONS)
    except Exception:
        logger.exception("[yopmail] chromium launch failed"); _yop_browser = None
    _yop_cleanup_task = asyncio.create_task(_cleanup_loop())

async def yopmail_shutdown():
    global _yop_browser, _yop_pw
    if _yop_cleanup_task: _yop_cleanup_task.cancel()
    async with _yop_store_lock:
        for s in list(_yop_sessions.values()): await _close_sess(s)
        _yop_sessions.clear()
    try:
        if _yop_browser: await _yop_browser.close()
        if _yop_pw: await _yop_pw.stop()
    except Exception: pass

async def _cleanup_loop():
    while True:
        try:
            await asyncio.sleep(YOPMAIL_CLEANUP_INTERVAL); now=time.time()
            async with _yop_store_lock:
                for s in [s for s in _yop_sessions.values() if now-s.last_active>YOPMAIL_SESSION_TTL]:
                    logger.info("[yopmail] session %s expired", s.id[:8])
                    await _close_sess(s); _yop_sessions.pop(s.id, None)
        except asyncio.CancelledError: return
        except Exception: logger.exception("[yopmail] cleanup error")

async def _close_sess(s):
    if s.closed: return
    s.closed = True
    try: await s.context.close()
    except Exception: pass
    finally:
        if _yop_semaphore:
            try: _yop_semaphore.release()
            except ValueError: pass

def _check_tok(t): return (not YOPMAIL_SESSION_TOKEN) or t == YOPMAIL_SESSION_TOKEN

async def _try_extract(sess, code_extractor):
    try:
        text = ""
        for frame in sess.page.frames:
            try:
                body = await frame.inner_text("body", timeout=1200)
                if body and len(body) > len(text): text = body
            except Exception: continue
        if not text:
            try: text = await sess.page.inner_text("#mail", timeout=1200)
            except Exception: text = ""
        if text: return code_extractor(text)
    except Exception: pass
    return None

def register_yopmail_routes(app: FastAPI, code_extractor):
    app.add_middleware(CORSMiddleware, allow_origins=YOPMAIL_CORS_ORIGINS, allow_credentials=False, allow_methods=["GET","POST","OPTIONS"], allow_headers=["*"])

    @app.post("/yopmail/start")
    async def yop_start(data: YopStartReq):
        if not _check_tok(data.token): return {"success":False,"message":"ไม่ได้รับอนุญาต"}
        if _yop_browser is None: return {"success":False,"message":"ระบบบราวเซอร์ยังไม่พร้อม"}
        email = str(data.email or "").replace(" ","").lower().strip()
        shortname = email.split("@")[0] if "@" in email else email
        if not shortname: return {"success":False,"message":"รูปแบบอีเมลไม่ถูกต้อง"}
        if _yop_semaphore.locked() and _yop_semaphore._value == 0:
            return {"success":False,"message":"ระบบกำลังใช้งานเต็ม กรุณารอสักครู่"}
        await _yop_semaphore.acquire()
        sid = base64.urlsafe_b64encode(os.urandom(12)).decode().rstrip("=")
        try:
            ctx = await _yop_browser.new_context(viewport={"width":YOPMAIL_VIEWPORT_W,"height":YOPMAIL_VIEWPORT_H}, user_agent=YOPMAIL_USER_AGENT, locale="en-US", device_scale_factor=1)
            page = await ctx.new_page(); page.set_default_timeout(YOPMAIL_NAV_TIMEOUT_MS)
            await page.goto("https://yopmail.com/en/", wait_until="domcontentloaded")
            try:
                await page.fill("#login", shortname, timeout=8000)
                await page.click("#refreshbut .md", timeout=5000)
            except Exception:
                await page.goto(f"https://yopmail.com/en/wm?login={urllib.parse.quote(shortname)}", wait_until="domcontentloaded")
            sess = YopmailSession(sid, email, shortname, ctx, page)
            async with _yop_store_lock: _yop_sessions[sid] = sess
            logger.info("[yopmail] session %s started for %s", sid[:8], shortname)
            return {"success":True,"session_id":sid,"view_w":YOPMAIL_VIEWPORT_W,"view_h":YOPMAIL_VIEWPORT_H}
        except Exception:
            logger.exception("[yopmail] start failed")
            try: _yop_semaphore.release()
            except ValueError: pass
            return {"success":False,"message":"เปิดบราวเซอร์ไม่สำเร็จ กรุณาลองใหม่"}

    @app.get("/yopmail/shot/{session_id}")
    async def yop_shot(session_id: str, token: str = ""):
        if not _check_tok(token): return {"success":False,"message":"ไม่ได้รับอนุญาต"}
        sess = _yop_sessions.get(session_id)
        if not sess or sess.closed: return {"success":False,"message":"เซสชันหมดอายุ","expired":True}
        async with sess.lock:
            sess.touch()
            try:
                code = await _try_extract(sess, code_extractor)
                if code:
                    return {"success":True,"found_code":True,"code":code,"title":"รหัสยืนยัน 6 หลัก" if len(code)==6 else "รหัสเข้าสู่ระบบ"}
                png = await sess.page.screenshot(type="jpeg", quality=55)
                return {"success":True,"found_code":False,"img":"data:image/jpeg;base64,"+base64.b64encode(png).decode(),"view_w":YOPMAIL_VIEWPORT_W,"view_h":YOPMAIL_VIEWPORT_H}
            except Exception:
                logger.exception("[yopmail] screenshot failed")
                return {"success":False,"message":"ถ่ายภาพหน้าจอไม่สำเร็จ"}

    @app.post("/yopmail/click/{session_id}")
    async def yop_click(session_id: str, data: YopClickReq):
        if not _check_tok(data.token): return {"success":False,"message":"ไม่ได้รับอนุญาต"}
        sess = _yop_sessions.get(session_id)
        if not sess or sess.closed: return {"success":False,"message":"เซสชันหมดอายุ","expired":True}
        async with sess.lock:
            sess.touch()
            try:
                sx = (YOPMAIL_VIEWPORT_W/data.view_w) if data.view_w else 1.0
                sy = (YOPMAIL_VIEWPORT_H/data.view_h) if data.view_h else 1.0
                await sess.page.mouse.click(max(0,min(YOPMAIL_VIEWPORT_W,data.x*sx)), max(0,min(YOPMAIL_VIEWPORT_H,data.y*sy)))
                return {"success":True}
            except Exception: return {"success":False,"message":"คลิกไม่สำเร็จ"}

    @app.post("/yopmail/refresh/{session_id}")
    async def yop_refresh(session_id: str, data: YopActionReq):
        if not _check_tok(data.token): return {"success":False,"message":"ไม่ได้รับอนุญาต"}
        sess = _yop_sessions.get(session_id)
        if not sess or sess.closed: return {"success":False,"message":"เซสชันหมดอายุ","expired":True}
        async with sess.lock:
            sess.touch()
            try: await sess.page.reload(wait_until="domcontentloaded"); return {"success":True}
            except Exception: return {"success":False,"message":"รีเฟรชไม่สำเร็จ"}

    @app.post("/yopmail/close/{session_id}")
    async def yop_close(session_id: str, data: YopActionReq):
        if not _check_tok(data.token): return {"success":False,"message":"ไม่ได้รับอนุญาต"}
        async with _yop_store_lock: sess = _yop_sessions.pop(session_id, None)
        if sess: await _close_sess(sess); logger.info("[yopmail] session %s closed", session_id[:8])
        return {"success":True}

# ═══════════════════════════════════════════════════════════
# วิธีใช้: เพิ่มใน main.py แค่ 4 บรรทัด (ไม่ต้องลบอะไร)
# ───────────────────────────────────────────────────────────
#
# จุดที่ 1: บนสุดของ main.py (ต่อท้าย import อื่นๆ) เพิ่ม:
#   from yopmail_browser import yopmail_startup, yopmail_shutdown, register_yopmail_routes
#
# จุดที่ 2: หลังบรรทัด app = FastAPI(...) เพิ่ม:
#   register_yopmail_routes(app, extract_code)
#
# จุดที่ 3: ท้าย async def startup() เพิ่ม:
#   await yopmail_startup()
#
# จุดที่ 4: ต้น async def shutdown() เพิ่ม:
#   await yopmail_shutdown()
#
# requirements.txt เพิ่ม: playwright
# Render build command:
#   pip install -r requirements.txt && playwright install --with-deps chromium
# ═══════════════════════════════════════════════════════════

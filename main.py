import os
import re
import json
import uuid
import html
import time
import asyncio
import logging
import urllib.parse
from contextlib import asynccontextmanager
from collections import defaultdict
from typing import Any, Dict, List, Optional, AsyncGenerator, Set

import httpx
from fastapi import FastAPI
from pydantic import BaseModel
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl


# =========================
# ENV CONFIG
# =========================
API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
TG_STRING_SESSION = os.getenv("TG_STRING_SESSION", "")

TIMEOUT_SECONDS = float(os.getenv("TIMEOUT_SECONDS", "35"))
SEMAPHORE_LIMIT = int(os.getenv("SEMAPHORE_LIMIT", "15"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.22"))
MESSAGE_LIMIT = int(os.getenv("MESSAGE_LIMIT", "18"))

SAFE_SAME_BOT_QUEUE = os.getenv("SAFE_SAME_BOT_QUEUE", "false").lower() == "true"
ALLOW_UNMATCHED_CONCURRENT = os.getenv("ALLOW_UNMATCHED_CONCURRENT", "true").lower() == "true"
USE_EVENT_LISTENER = os.getenv("USE_EVENT_LISTENER", "true").lower() == "true"
USE_POLLING_FALLBACK = os.getenv("USE_POLLING_FALLBACK", "true").lower() == "true"

SPECIAL_BOT = "@faultyhhbot"

# ---- Bhagatflix (Magic Window) ----
BHAGATFLIX_BOT = "@bhagatflix"
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://arjzgyadqemequykgvcz.supabase.co")
SUPABASE_ANON_KEY = os.getenv(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFyanpneWFkcWVtZXF1eWtndmN6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTI1MDUzNDIsImV4cCI6MjA2ODA4MTM0Mn0.72VjrbubOyq0rtGjRAwjixfRtAQQFUJHKpxI6wnh1Tk"
)
SUPABASE_PROJECT_ID = os.getenv("SUPABASE_PROJECT_ID", "arjzgyadqemequykgvcz")
BHAGATFLIX_EMAIL = os.getenv("BHAGATFLIX_EMAIL", "")
BHAGATFLIX_PASSWORD = os.getenv("BHAGATFLIX_PASSWORD", "")
BHAGATFLIX_BASE = os.getenv("BHAGATFLIX_BASE", "https://www.bhagatflix.com")

BHAGATFLIX_ENDPOINTS = {
    "code": "/api/signin-code",
    "household": "/api/household-code",
    "reset": "/api/reset-link",
}

_bhagat_token_cache: Dict[str, Any] = {"access_token": None, "refresh_token": None, "expires_at": 0}
_bhagat_token_lock = asyncio.Lock()


# =========================
# APP / CLIENT
# =========================
app = FastAPI(title="OTP Python Server Final")

client = TelegramClient(
    StringSession(TG_STRING_SESSION),
    API_ID,
    API_HASH
)

semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
entity_cache: Dict[str, Any] = {}
bot_locks: Dict[str, asyncio.Lock] = {}
active_by_bot: Dict[str, int] = defaultdict(int)

pending_requests: Dict[str, Dict[str, Any]] = {}
pending_lock = asyncio.Lock()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("otp-server-final")


# =========================
# MODELS
# =========================
class OtpRequest(BaseModel):
    email: str
    botUsername: str


class ButtonRequest(BaseModel):
    email: str
    botUsername: str
    row: int = 0
    col: int = 0
    buttonText: str = ""
    messageId: int = 0


# =========================
# STARTUP / SHUTDOWN
# =========================
@app.on_event("startup")
async def startup() -> None:
    if not API_ID or not API_HASH or not TG_STRING_SESSION:
        logger.warning("Missing required Telegram environment variables")
    try:
        await client.connect()
    except Exception:
        logger.exception("telegram connect failed")

    if USE_EVENT_LISTENER:
        try:
            register_event_listener()
        except Exception:
            logger.exception("event listener register failed")

    logger.info(
        "server startup complete bhagatflix_email_set=%s",
        bool(BHAGATFLIX_EMAIL and BHAGATFLIX_PASSWORD)
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    try:
        await client.disconnect()
    except Exception:
        pass


# =========================
# ROUTES
# =========================
@app.get("/")
async def home() -> Dict[str, Any]:
    return {"success": True, "status": "running"}


@app.get("/health")
async def health() -> Dict[str, Any]:
    request_id = make_request_id()
    try:
        connected = client.is_connected()
        authorized = await client.is_user_authorized() if connected else False
        return {
            "success": True,
            "status": "ok" if connected and authorized else "not_ready",
            "connected": connected,
            "authorized": authorized,
            "bhagatflixReady": bool(BHAGATFLIX_EMAIL and BHAGATFLIX_PASSWORD),
            "pendingRequests": len(pending_requests),
            "requestId": request_id
        }
    except Exception as exc:
        logger.exception("health failed")
        return {"success": False, "status": "error", "message": sanitize_error(exc), "requestId": request_id}


@app.post("/get-otp")
async def get_otp(data: OtpRequest) -> Dict[str, Any]:
    request_id = make_request_id()
    email = clean_email(data.email)
    bot_username = normalize_bot_username(data.botUsername)

    logger.info("get_otp start requestId=%s email=%s system=%s", request_id, mask_email(email), bot_username)

    if not email:
        return fail("กรุณากรอกอีเมล", request_id)
    if not bot_username:
        return fail("กรุณาเลือกระบบ", request_id)
    if not is_valid_email(email):
        return fail("รูปแบบอีเมลไม่ถูกต้อง", request_id)

    # ---- Bhagatflix special: ส่งเมนู 3 ปุ่มกลับไปทันที ----
    if is_bhagatflix(bot_username):
        return {
            "success": False,
            "needButton": True,
            "message": "กรุณาเลือกเมนูที่ต้องการ",
            "buttons": [
                {"text": "ขอโค้ดเข้าสู่ระบบ", "row": 0, "col": 0},
                {"text": "ยืนยันครัวเรือน", "row": 0, "col": 1},
                {"text": "ลิงก์รีเซ็ตรหัสผ่าน", "row": 0, "col": 2}
            ],
            "messageId": 0,
            "specialMode": True,
            "magicWindow": True,
            "requestId": request_id
        }

    # ---- Telegram flow ----
    async with semaphore:
        async with active_bot_request(bot_username):
            try:
                await ensure_client_ready()

                if should_use_special_bot(bot_username):
                    return {
                        "success": False,
                        "needButton": True,
                        "message": "กรุณาเลือกเมนูที่ต้องการ",
                        "buttons": [
                            {"text": "ขอโค้ดเข้าสู่ระบบ", "row": 0, "col": 0},
                            {"text": "ยืนยันครัวเรือน", "row": 0, "col": 1},
                            {"text": "ลิงก์รีเซ็ตรหัสผ่าน", "row": 0, "col": 2}
                        ],
                        "messageId": 0,
                        "specialMode": True,
                        "requestId": request_id
                    }

                target = await get_cached_entity(bot_username)

                async with optional_bot_lock(bot_username):
                    sent_msg = await client.send_message(target, email)

                result = await wait_for_buttons_or_result(
                    target=target, bot_username=bot_username, after_id=sent_msg.id,
                    email=email, selected_button="ขอโค้ดเข้าสู่ระบบ",
                    request_id=request_id, expect_buttons=True, special_mode=False
                )
                return result
            except Exception as exc:
                logger.exception("get_otp error")
                return fail(sanitize_error(exc), request_id)


@app.post("/click-button")
async def click_button(data: ButtonRequest) -> Dict[str, Any]:
    request_id = make_request_id()
    email = clean_email(data.email)
    bot_username = normalize_bot_username(data.botUsername)
    button_text = clean_text(data.buttonText)

    if not email:
        return fail("กรุณากรอกอีเมล", request_id)
    if not bot_username:
        return fail("กรุณาเลือกระบบ", request_id)
    if not is_valid_email(email):
        return fail("รูปแบบอีเมลไม่ถูกต้อง", request_id)

    # ---- Bhagatflix special ----
    if is_bhagatflix(bot_username):
        return await handle_bhagatflix_click(
            email=email, row=data.row, col=data.col,
            button_text=button_text, request_id=request_id
        )

    # ---- Telegram flow ----
    async with semaphore:
        async with active_bot_request(bot_username):
            try:
                await ensure_client_ready()
                target = await get_cached_entity(bot_username)

                if should_use_special_bot(bot_username):
                    command_text = build_special_command(
                        button_text=button_text, email=email,
                        row=data.row, col=data.col
                    )
                    if not command_text:
                        return fail("ไม่รู้จักเมนูที่เลือก กรุณาลองใหม่อีกครั้ง", request_id)

                    async with optional_bot_lock(bot_username):
                        sent_msg = await client.send_message(target, command_text)

                    return await wait_for_buttons_or_result(
                        target=target, bot_username=bot_username, after_id=sent_msg.id,
                        email=email,
                        selected_button=button_text or special_title_from_position(data.row, data.col),
                        request_id=request_id, expect_buttons=False, special_mode=True
                    )

                target_msg = await find_button_message(target=target, message_id=data.messageId, email=email)
                if not target_msg:
                    return fail("ไม่พบเมนู กรุณาลองใหม่อีกครั้ง", request_id)

                clicked = await click_target_button(
                    msg=target_msg, row=data.row, col=data.col, button_text=button_text
                )
                if not clicked:
                    return fail("กดเมนูไม่สำเร็จ กรุณาลองใหม่อีกครั้ง", request_id)

                return await wait_for_buttons_or_result(
                    target=target, bot_username=bot_username, after_id=target_msg.id,
                    email=email, selected_button=button_text,
                    request_id=request_id, expect_buttons=False, special_mode=False
                )
            except Exception as exc:
                logger.exception("click_button error")
                return fail(sanitize_error(exc), request_id)


# =========================
# BHAGATFLIX (Magic Window)
# =========================
def is_bhagatflix(bot_username: str) -> bool:
    return normalize_bot_username(bot_username) == BHAGATFLIX_BOT


def bhagatflix_action_from_position(row: int, col: int, button_text: str) -> Optional[str]:
    text = clean_text(button_text).lower()
    if text:
        if is_code_choice(text):
            return "code"
        if is_household_choice(text):
            return "household"
        if is_reset_choice(text):
            return "reset"
    if row == 0 and col == 0:
        return "code"
    if row == 0 and col == 1:
        return "household"
    if row == 0 and col == 2:
        return "reset"
    return None


def bhagatflix_title(action: str) -> str:
    return {
        "code": "ขอโค้ดเข้าสู่ระบบ",
        "household": "ยืนยันครัวเรือน",
        "reset": "ลิงก์รีเซ็ตรหัสผ่าน",
    }.get(action, "ข้อมูล")


async def get_bhagatflix_token() -> Optional[Dict[str, Any]]:
    if not BHAGATFLIX_EMAIL or not BHAGATFLIX_PASSWORD:
        return None

    async with _bhagat_token_lock:
        now = time.time()
        cached = _bhagat_token_cache
        if cached.get("access_token") and cached.get("expires_at", 0) > now + 30:
            return cached

        url = f"{SUPABASE_URL}/auth/v1/token?grant_type=password"
        headers = {
            "Content-Type": "application/json",
            "apikey": SUPABASE_ANON_KEY,
            "Origin": BHAGATFLIX_BASE,
            "Referer": BHAGATFLIX_BASE + "/",
        }
        payload = {"email": BHAGATFLIX_EMAIL, "password": BHAGATFLIX_PASSWORD}

        try:
            async with httpx.AsyncClient(timeout=15.0) as http:
                resp = await http.post(url, json=payload, headers=headers)
            if resp.status_code != 200:
                return None
            data = resp.json()
            expires_in = int(data.get("expires_in") or 3600)
            cached.update({
                "access_token": data.get("access_token"),
                "refresh_token": data.get("refresh_token"),
                "expires_at": now + expires_in,
                "user": data.get("user"),
                "expires_in": expires_in,
                "token_type": data.get("token_type", "bearer"),
            })
            return cached
        except Exception:
            logger.exception("bhagatflix supabase login error")
            return None


def build_bhagatflix_cookies(token_data: Dict[str, Any]) -> Dict[str, str]:
    cookie_obj = {
        "access_token": token_data.get("access_token"),
        "refresh_token": token_data.get("refresh_token"),
        "expires_in": token_data.get("expires_in", 3600),
        "expires_at": int(token_data.get("expires_at", 0)),
        "token_type": token_data.get("token_type", "bearer"),
        "user": token_data.get("user"),
    }
    cookie_json = json.dumps(cookie_obj, separators=(",", ":"))
    cookie_encoded = urllib.parse.quote(cookie_json, safe="")
    cookies = {f"sb-{SUPABASE_PROJECT_ID}-auth-token": cookie_encoded}
    chunk_size = 3000
    if len(cookie_encoded) > chunk_size:
        chunks = [cookie_encoded[i:i + chunk_size] for i in range(0, len(cookie_encoded), chunk_size)]
        for i, chunk in enumerate(chunks):
            cookies[f"sb-{SUPABASE_PROJECT_ID}-auth-token.{i}"] = chunk
    return cookies


async def call_bhagatflix_api(action: str, customer_email: str) -> Dict[str, Any]:
    endpoint = BHAGATFLIX_ENDPOINTS.get(action)
    if not endpoint:
        return {"ok": False, "error": "unknown action"}

    token_data = await get_bhagatflix_token()
    if not token_data or not token_data.get("access_token"):
        return {"ok": False, "error": "auth failed"}

    url = f"{BHAGATFLIX_BASE}{endpoint}"
    cookies = build_bhagatflix_cookies(token_data)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token_data['access_token']}",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Origin": BHAGATFLIX_BASE,
        "Referer": BHAGATFLIX_BASE + "/",
        "Accept": "application/json, text/plain, */*",
    }
    payload = {"email": customer_email}

    try:
        async with httpx.AsyncClient(timeout=20.0) as http:
            resp = await http.post(url, json=payload, headers=headers, cookies=cookies)
        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text[:500]}
        return {"ok": resp.status_code == 200, "status": resp.status_code, "data": data}
    except Exception as exc:
        logger.exception("bhagatflix api error")
        return {"ok": False, "error": str(exc)}


def parse_bhagatflix_response(action: str, raw: Dict[str, Any], request_id: str, customer_email: str) -> Dict[str, Any]:
    title = bhagatflix_title(action)

    if not raw.get("ok"):
        msg = "ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง"
        data = raw.get("data") or {}
        if isinstance(data, dict):
            err = data.get("error") or data.get("message")
            if err:
                err_lower = str(err).lower()
                if "not authenticated" in err_lower or "unauthorized" in err_lower:
                    msg = "ระบบยังไม่พร้อมใช้งาน กรุณาติดต่อผู้ดูแล"
        return fail(msg, request_id)

    data = raw.get("data") or {}
    emails = data.get("emails") if isinstance(data, dict) else None

    if isinstance(emails, list) and emails:
        first = emails[0] or {}
        html_body = first.get("html") or first.get("body") or ""
        subject = first.get("subject") or title

        if action == "reset":
            link = extract_netflix_link(html_body)
            if link:
                return {
                    "success": True, "type": "link", "title": title, "value": link,
                    "message": subject, "subject": subject, "html": html_body,
                    "email": customer_email, "magicWindow": True, "requestId": request_id
                }
        else:
            code = extract_netflix_code(html_body)
            if code:
                return {
                    "success": True, "type": "code", "title": title, "value": code,
                    "message": subject, "subject": subject, "html": html_body,
                    "email": customer_email, "magicWindow": True, "requestId": request_id
                }

        # fallback: เจอเมลแต่หา code/link ไม่เจอ ส่ง html ไปให้แสดงเอง
        return {
            "success": True, "type": "email", "title": title, "value": "",
            "message": subject, "subject": subject, "html": html_body,
            "email": customer_email, "magicWindow": True, "requestId": request_id
        }

    if isinstance(data, dict):
        for key in ("code", "otp", "signin_code", "household_code", "token"):
            if data.get(key):
                return {
                    "success": True, "type": "code", "title": title,
                    "value": str(data[key]), "message": title,
                    "email": customer_email, "magicWindow": True, "requestId": request_id
                }
        for key in ("link", "reset_link", "url", "reset_url"):
            if data.get(key):
                return {
                    "success": True, "type": "link", "title": title,
                    "value": str(data[key]), "message": title,
                    "email": customer_email, "magicWindow": True, "requestId": request_id
                }

    return fail("ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง", request_id)


def extract_netflix_code(html_body: str) -> Optional[str]:
    if not html_body:
        return None
    text = re.sub(r"<style[^>]*>[\s\S]*?</style>", " ", html_body, flags=re.IGNORECASE)
    text = re.sub(r"<script[^>]*>[\s\S]*?</script>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()

    keyword_patterns = [
        r"ป้อนรหัสนี้เพื่อเข้าสู่ระบบ\s*([0-9]{4,8})",
        r"ป้อนรหัส[^0-9]{0,40}([0-9]{4,8})",
        r"เพื่อเข้าใช้งาน[^0-9]{0,40}([0-9]{4,8})",
        r"รหัสยืนยัน[^0-9]{0,40}([0-9]{4,8})",
        r"sign[\s-]*in\s*code[^0-9]{0,40}([0-9]{4,8})",
        r"verification\s*code[^0-9]{0,40}([0-9]{4,8})",
        r"travel\s*verify\s*code[^0-9]{0,40}([0-9]{4,8})",
        r"household\s*code[^0-9]{0,40}([0-9]{4,8})",
        r"enter\s*this\s*code[^0-9]{0,40}([0-9]{4,8})",
    ]
    for pattern in keyword_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)

    match = re.search(r"(?<!\d)([0-9]{4})(?!\d)", text)
    if match:
        return match.group(1)
    return None


def extract_netflix_link(html_body: str) -> Optional[str]:
    if not html_body:
        return None
    patterns = [
        r"href=[\"']([^\"']*(?:netflix|nflxext)[^\"']*reset[^\"']*)[\"']",
        r"href=[\"']([^\"']*reset[^\"']*(?:netflix|nflxext)[^\"']*)[\"']",
        r"href=[\"']([^\"']*(?:netflix|nflxext)[^\"']*)[\"']",
    ]
    for pattern in patterns:
        match = re.search(pattern, html_body, re.IGNORECASE)
        if match:
            return match.group(1).rstrip(".,;)]}")
    match = re.search(r"https?://[^\s\"'<>]+", html_body)
    if match:
        return match.group(0).rstrip(".,;)]}")
    return None


async def handle_bhagatflix_click(
    email: str, row: int, col: int, button_text: str, request_id: str
) -> Dict[str, Any]:
    action = bhagatflix_action_from_position(row, col, button_text)
    if not action:
        return fail("ไม่รู้จักเมนูที่เลือก กรุณาลองใหม่อีกครั้ง", request_id)

    raw = await call_bhagatflix_api(action, email)
    return parse_bhagatflix_response(action, raw, request_id, email)


# =========================
# EVENT LISTENER (TELEGRAM)
# =========================
def register_event_listener() -> None:
    @client.on(events.NewMessage(incoming=True))
    async def on_new_message(event: Any) -> None:
        try:
            msg = event.message
            if not msg:
                return
            await dispatch_incoming_message(msg)
        except Exception:
            logger.exception("event listener failed")


async def dispatch_incoming_message(msg: Any) -> None:
    async with pending_lock:
        if not pending_requests:
            return
        matched_keys: List[str] = []
        items = list(pending_requests.items())
        for key, pending in items:
            if pending.get("done"):
                continue
            after_id = int(pending.get("after_id") or 0)
            if msg.id <= after_id:
                continue
            result = build_result_from_message(
                msg=msg, bot_username=pending["bot_username"], email=pending["email"],
                selected_button=pending["selected_button"], request_id=pending["request_id"],
                expect_buttons=pending["expect_buttons"], special_mode=pending["special_mode"]
            )
            if not result:
                continue
            if not is_relevant_message(
                msg=msg, bot_username=pending["bot_username"], email=pending["email"],
                selected_button=pending["selected_button"], special_mode=pending["special_mode"]
            ):
                continue
            future: asyncio.Future = pending["future"]
            if not future.done():
                future.set_result(result)
                pending["done"] = True
                matched_keys.append(key)
        for key in matched_keys:
            pending_requests.pop(key, None)


async def wait_for_buttons_or_result(
    target: Any, bot_username: str, after_id: int, email: str,
    selected_button: str, request_id: str, expect_buttons: bool, special_mode: bool
) -> Dict[str, Any]:
    if USE_EVENT_LISTENER:
        result = await wait_with_event_listener(
            target=target, bot_username=bot_username, after_id=after_id, email=email,
            selected_button=selected_button, request_id=request_id,
            expect_buttons=expect_buttons, special_mode=special_mode
        )
        if result:
            return result

    return await wait_with_polling(
        target=target, bot_username=bot_username, after_id=after_id, email=email,
        selected_button=selected_button, request_id=request_id,
        expect_buttons=expect_buttons, special_mode=special_mode
    )


async def wait_with_event_listener(
    target: Any, bot_username: str, after_id: int, email: str,
    selected_button: str, request_id: str, expect_buttons: bool, special_mode: bool
) -> Optional[Dict[str, Any]]:
    loop = asyncio.get_event_loop()
    future: asyncio.Future = loop.create_future()
    pending_key = request_id
    target_id = get_entity_identity(target)

    async with pending_lock:
        pending_requests[pending_key] = {
            "future": future, "request_id": request_id, "bot_username": bot_username,
            "target_id": target_id, "after_id": after_id, "email": email,
            "selected_button": selected_button, "expect_buttons": expect_buttons,
            "special_mode": special_mode, "created_at": time.time(), "done": False
        }

    polling_task: Optional[asyncio.Task] = None
    if USE_POLLING_FALLBACK:
        polling_task = asyncio.create_task(
            polling_fallback_to_future(
                future=future, target=target, bot_username=bot_username,
                after_id=after_id, email=email, selected_button=selected_button,
                request_id=request_id, expect_buttons=expect_buttons, special_mode=special_mode
            )
        )

    try:
        return await asyncio.wait_for(future, timeout=TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        return fail("ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง", request_id)
    finally:
        async with pending_lock:
            pending_requests.pop(pending_key, None)
        if polling_task and not polling_task.done():
            polling_task.cancel()


async def polling_fallback_to_future(
    future: asyncio.Future, target: Any, bot_username: str, after_id: int,
    email: str, selected_button: str, request_id: str,
    expect_buttons: bool, special_mode: bool
) -> None:
    try:
        start_time = asyncio.get_event_loop().time()
        while True:
            if future.done():
                return
            if asyncio.get_event_loop().time() - start_time > TIMEOUT_SECONDS:
                return
            messages = await get_new_messages(target, after_id)
            for msg in messages:
                if future.done():
                    return
                result = build_result_from_message(
                    msg=msg, bot_username=bot_username, email=email,
                    selected_button=selected_button, request_id=request_id,
                    expect_buttons=expect_buttons, special_mode=special_mode
                )
                if not result:
                    continue
                if not is_relevant_message(
                    msg=msg, bot_username=bot_username, email=email,
                    selected_button=selected_button, special_mode=special_mode
                ):
                    continue
                future.set_result(result)
                return
            await asyncio.sleep(POLL_INTERVAL)
    except asyncio.CancelledError:
        return
    except Exception:
        logger.exception("polling fallback failed")


async def wait_with_polling(
    target: Any, bot_username: str, after_id: int, email: str,
    selected_button: str, request_id: str, expect_buttons: bool, special_mode: bool
) -> Dict[str, Any]:
    start_time = asyncio.get_event_loop().time()
    while True:
        if asyncio.get_event_loop().time() - start_time > TIMEOUT_SECONDS:
            return fail("ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง", request_id)

        messages = await get_new_messages(target, after_id)
        for msg in messages:
            result = build_result_from_message(
                msg=msg, bot_username=bot_username, email=email,
                selected_button=selected_button, request_id=request_id,
                expect_buttons=expect_buttons, special_mode=special_mode
            )
            if not result:
                continue
            if is_relevant_message(
                msg=msg, bot_username=bot_username, email=email,
                selected_button=selected_button, special_mode=special_mode
            ):
                return result
        await asyncio.sleep(POLL_INTERVAL)


def build_result_from_message(
    msg: Any, bot_username: str, email: str, selected_button: str,
    request_id: str, expect_buttons: bool, special_mode: bool
) -> Optional[Dict[str, Any]]:
    if expect_buttons and getattr(msg, "buttons", None):
        return {
            "success": False, "needButton": True,
            "message": "กรุณาเลือกเมนูที่ต้องการ",
            "buttons": extract_buttons(msg), "messageId": msg.id, "requestId": request_id
        }

    no_data = extract_no_data_message(msg.message or "")
    if no_data:
        return fail(no_data, request_id)

    return extract_code_or_link(msg=msg, selected_button=selected_button, request_id=request_id)


async def get_new_messages(target: Any, after_id: int) -> List[Any]:
    messages = await client.get_messages(target, limit=MESSAGE_LIMIT)
    new_messages = [m for m in messages if m and m.id > after_id]
    new_messages.sort(key=lambda item: item.id)
    return new_messages


# =========================
# TELEGRAM HELPERS
# =========================
async def ensure_client_ready() -> None:
    if not client.is_connected():
        await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("ระบบยังไม่พร้อมใช้งาน กรุณาติดต่อผู้ดูแล")


async def get_cached_entity(bot_username: str) -> Any:
    key = normalize_bot_username(bot_username)
    if key in entity_cache:
        return entity_cache[key]
    entity = await client.get_entity(key)
    entity_cache[key] = entity
    return entity


def get_entity_identity(entity: Any) -> Optional[str]:
    try:
        entity_id = getattr(entity, "id", None)
        if entity_id is not None:
            return str(entity_id)
    except Exception:
        pass
    return None


@asynccontextmanager
async def active_bot_request(bot_username: str) -> AsyncGenerator[None, None]:
    key = normalize_bot_username(bot_username)
    active_by_bot[key] += 1
    try:
        yield
    finally:
        active_by_bot[key] = max(0, active_by_bot[key] - 1)


@asynccontextmanager
async def optional_bot_lock(bot_username: str) -> AsyncGenerator[None, None]:
    key = normalize_bot_username(bot_username)
    if not SAFE_SAME_BOT_QUEUE:
        yield
        return
    if key not in bot_locks:
        bot_locks[key] = asyncio.Lock()
    async with bot_locks[key]:
        yield


async def find_button_message(target: Any, message_id: int = 0, email: str = "") -> Optional[Any]:
    if message_id:
        try:
            msg = await client.get_messages(target, ids=message_id)
            if msg and getattr(msg, "buttons", None):
                return msg
        except Exception:
            pass

    messages = await client.get_messages(target, limit=MESSAGE_LIMIT)
    email_lower = clean_email(email)
    fallback_msg = None
    for msg in messages:
        if not getattr(msg, "buttons", None):
            continue
        text = clean_text(msg.message).lower()
        if email_lower and email_lower in text:
            return msg
        if fallback_msg is None:
            fallback_msg = msg
    return fallback_msg


async def click_target_button(msg: Any, row: int = 0, col: int = 0, button_text: str = "") -> bool:
    button_text = clean_text(button_text).lower()
    try:
        if button_text:
            for row_index, button_row in enumerate(msg.buttons or []):
                for col_index, button in enumerate(button_row):
                    current_text = clean_text(getattr(button, "text", "")).lower()
                    if (current_text == button_text or button_text in current_text or current_text in button_text):
                        await msg.click(row_index, col_index)
                        return True
        await msg.click(row, col)
        return True
    except Exception:
        return False


# =========================
# MATCHING / EXTRACTION
# =========================
def is_relevant_message(msg: Any, bot_username: str, email: str, selected_button: str, special_mode: bool = False) -> bool:
    text = html.unescape(msg.message or "")
    text_lower = clean_text(text).lower()
    email_lower = clean_email(email)
    bot_key = normalize_bot_username(bot_username)
    active_count = active_by_bot.get(bot_key, 0)

    if email_lower and email_lower in text_lower:
        return True

    selected_lower = clean_text(selected_button).lower()
    no_data = extract_no_data_message(text)
    if no_data and email_lower and email_lower in text_lower:
        return True

    if active_count <= 1:
        return True

    if selected_lower:
        if is_reset_choice(selected_lower):
            if "reset" in text_lower or "password" in text_lower or extract_urls_from_message(msg):
                return ALLOW_UNMATCHED_CONCURRENT
        if is_household_choice(selected_lower):
            if "travel verify" in text_lower or "household" in text_lower or "travel" in text_lower or "verify" in text_lower:
                return ALLOW_UNMATCHED_CONCURRENT
        if is_code_choice(selected_lower):
            if looks_like_code_message(text_lower):
                return ALLOW_UNMATCHED_CONCURRENT

    if special_mode and ALLOW_UNMATCHED_CONCURRENT:
        return True

    return False


def extract_code_or_link(msg: Any, selected_button: str, request_id: str) -> Optional[Dict[str, Any]]:
    text = html.unescape(msg.message or "")

    code = extract_code(text)
    if code:
        return {
            "success": True, "type": "code",
            "title": selected_button or detect_title_from_text(text),
            "value": code, "message": text, "requestId": request_id
        }

    urls = extract_urls_from_message(msg)
    if urls:
        selected_url = select_url_for_result(urls=urls, text=text, selected_button=selected_button)
        return {
            "success": True, "type": "link",
            "title": selected_button or detect_title_from_text(text),
            "value": selected_url, "message": text, "requestId": request_id
        }

    return None


def extract_code(text: str) -> Optional[str]:
    if not text:
        return None
    text = html.unescape(text)

    specific_patterns = [
        r"Netflix\s*Sign[-\s]*in\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"Netflix\s*Travel\s*Verify\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)"
    ]
    for pattern in specific_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            code = extract_first_4_digit_code(match.group(1))
            if code:
                return code

    label_patterns = [
        r"OTP\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"Verification\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"Login\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"Sign[-\s]*in\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"Travel\s*Verify\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"รหัส\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"โค้ด\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"ยืนยัน\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)"
    ]
    for pattern in label_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            code_4 = extract_first_4_digit_code(match.group(1))
            if code_4:
                return code_4
            code_any = extract_first_4_to_8_digit_code(match.group(1))
            if code_any:
                return code_any

    if looks_like_code_message(text):
        code_4 = extract_first_4_digit_code(text)
        if code_4:
            return code_4
        code_any = extract_first_4_to_8_digit_code(text)
        if code_any:
            return code_any

    return None


def extract_first_4_digit_code(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.search(r"(?<!\d)([0-9]{4})(?!\d)", text)
    return match.group(1) if match else None


def extract_first_4_to_8_digit_code(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.search(r"(?<!\d)([0-9]{4,8})(?!\d)", text)
    return match.group(1) if match else None


def extract_no_data_message(text: str) -> Optional[str]:
    value = clean_text(html.unescape(text)).lower()
    no_data_patterns = [
        "no new emails found", "no matching email found", "no new data found",
        "new data failed", "not found", "no data", "no email",
        "ไม่พบข้อมูล", "ไม่พบอีเมล"
    ]
    if any(pattern in value for pattern in no_data_patterns):
        return "ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง"
    return None


def extract_urls_from_message(msg: Any) -> List[str]:
    urls: List[str] = []
    text = html.unescape(msg.message or "")
    entities = getattr(msg, "entities", None) or []

    for entity in entities:
        if isinstance(entity, MessageEntityTextUrl):
            url = getattr(entity, "url", None)
            if url:
                urls.append(url)
        elif isinstance(entity, MessageEntityUrl):
            try:
                start = entity.offset
                end = entity.offset + entity.length
                raw_url = text[start:end]
                if raw_url:
                    urls.append(raw_url)
            except Exception:
                pass

    raw_url_matches = re.findall(r"https?://[^\s<>\]\)\"']+", text, re.IGNORECASE)
    for url in raw_url_matches:
        urls.append(url)

    if getattr(msg, "buttons", None):
        for row in msg.buttons:
            for button in row:
                url = getattr(button, "url", None)
                if url:
                    urls.append(url)

    return unique_list(clean_url(url) for url in urls if url)


def select_url_for_result(urls: List[str], text: str, selected_button: str) -> str:
    if not urls:
        return ""
    return urls[0]


def detect_title_from_text(text: str) -> str:
    value = clean_text(text).lower()
    if "travel verify code" in value or "household" in value or "ครัวเรือน" in value:
        return "ยืนยันครัวเรือน"
    if "sign-in code" in value or "signin code" in value or "sign in code" in value:
        return "ขอโค้ดเข้าสู่ระบบ"
    if "password reset" in value or "reset link" in value:
        return "ลิงก์รีเซ็ตรหัสผ่าน"
    return "ข้อมูลล่าสุด"


def looks_like_code_message(text: str) -> bool:
    value = clean_text(text).lower()
    keywords = [
        "code", "otp", "verification", "verify", "login", "signin", "sign in", "sign-in",
        "travel verify", "netflix sign-in code", "netflix travel verify code",
        "เข้าสู่ระบบ", "รหัส", "โค้ด", "ยืนยัน"
    ]
    return any(keyword in value for keyword in keywords)


def extract_buttons(message: Any) -> List[Dict[str, Any]]:
    buttons: List[Dict[str, Any]] = []
    for row_index, row in enumerate(message.buttons or []):
        for col_index, button in enumerate(row):
            buttons.append({
                "text": clean_text(getattr(button, "text", "")),
                "row": row_index, "col": col_index
            })
    return buttons


# =========================
# SPECIAL BOT (FAULTYHHBOT)
# =========================
def build_special_command(button_text: str, email: str, row: int = 0, col: int = 0) -> Optional[str]:
    text = clean_text(button_text).lower()
    if not text:
        if row == 0 and col == 0:
            return f"/code {email}"
        if row == 0 and col == 1:
            return f"/link {email}"
        if row == 0 and col == 2:
            return f"/pwlink {email}"

    if is_code_choice(text):
        return f"/code {email}"
    if is_household_choice(text):
        return f"/link {email}"
    if is_reset_choice(text):
        return f"/pwlink {email}"
    return None


def special_title_from_position(row: int, col: int) -> str:
    if row == 0 and col == 0:
        return "ขอโค้ดเข้าสู่ระบบ"
    if row == 0 and col == 1:
        return "ยืนยันครัวเรือน"
    if row == 0 and col == 2:
        return "ลิงก์รีเซ็ตรหัสผ่าน"
    return "ข้อมูล"


def is_code_choice(text: str) -> bool:
    value = clean_text(text).lower()
    return ("เข้าสู่ระบบ" in value or "โค้ด" in value or "code" in value
            or "signin" in value or "sign in" in value or "sign-in" in value)


def is_household_choice(text: str) -> bool:
    value = clean_text(text).lower()
    return ("ครัวเรือน" in value or "household" in value or "travel" in value or "verify" in value)


def is_reset_choice(text: str) -> bool:
    value = clean_text(text).lower()
    return ("รีเซ็ต" in value or "reset" in value or "pwlink" in value or "password" in value)


def normalize_bot_username(bot_username: Any) -> str:
    value = clean_text(bot_username).lower()
    if not value:
        return ""
    if not value.startswith("@"):
        value = "@" + value
    return value


def should_use_special_bot(bot_username: str) -> bool:
    return normalize_bot_username(bot_username) == SPECIAL_BOT


# =========================
# UTILS
# =========================
def is_valid_email(email: str) -> bool:
    return bool(re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email))


def make_request_id() -> str:
    return uuid.uuid4().hex[:12]


def mask_email(email: str) -> str:
    email = clean_email(email)
    if "@" not in email:
        return "***"
    name, domain = email.split("@", 1)
    if len(name) <= 2:
        masked_name = name[0:1] + "***"
    else:
        masked_name = name[:2] + "***" + name[-1:]
    return masked_name + "@" + domain


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def clean_email(value: Any) -> str:
    return clean_text(value).replace(" ", "").lower()


def clean_url(url: str) -> str:
    return clean_text(url).rstrip(".,;)]}")


def unique_list(items: Any) -> List[str]:
    seen: Set[str] = set()
    result: List[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def sanitize_error(error: Any) -> str:
    raw = clean_text(error)
    replacements = [
        (r"telegram", "ระบบ"), (r"telethon", "ระบบ"),
        (r"botusername", "ระบบ"), (r"\bbot\b", "ระบบ"),
        (r"maker", "ระบบ"), (r"stringsession", "ระบบ"),
        (r"api_id", "ระบบ"), (r"api_hash", "ระบบ"),
        (r"traceback", "ระบบ"), (r"exception", "ระบบ"),
        (r"supabase", "ระบบ"), (r"bhagatflix", "ระบบ"),
    ]
    for pattern, repl in replacements:
        raw = re.sub(pattern, repl, raw, flags=re.IGNORECASE)

    no_data = extract_no_data_message(raw)
    if no_data:
        return no_data

    if not raw or len(raw) > 180:
        return "เกิดข้อผิดพลาด กรุณาลองใหม่อีกครั้ง"

    return raw


def fail(message: str, request_id: str = "") -> Dict[str, Any]:
    return {
        "success": False,
        "message": sanitize_error(message),
        "requestId": request_id or make_request_id()
    }

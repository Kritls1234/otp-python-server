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
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, AsyncGenerator, Set

import httpx
from fastapi import FastAPI
from pydantic import BaseModel
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl
from yopmail_browser import yopmail_startup, yopmail_shutdown, register_yopmail_routes

# =========================
# ENV CONFIG
# =========================
API_ID_1            = int(os.getenv("TG_API_ID", "0"))
API_HASH_1          = os.getenv("TG_API_HASH", "")
TG_STRING_SESSION_1 = os.getenv("TG_STRING_SESSION", "")

API_ID_2            = int(os.getenv("TG_API_ID_2", "0"))
API_HASH_2          = os.getenv("TG_API_HASH_2", "")
TG_STRING_SESSION_2 = os.getenv("TG_STRING_SESSION_2", "")

TIMEOUT_SECONDS             = float(os.getenv("TIMEOUT_SECONDS", "35"))
TIMEOUT_SECONDS_ACCOUNT1    = float(os.getenv("TIMEOUT_SECONDS_ACCOUNT1", str(TIMEOUT_SECONDS)))
TIMEOUT_SECONDS_ACCOUNT2    = float(os.getenv("TIMEOUT_SECONDS_ACCOUNT2", "90"))
SEMAPHORE_LIMIT             = int(os.getenv("SEMAPHORE_LIMIT", "15"))
POLL_INTERVAL               = float(os.getenv("POLL_INTERVAL", "0.22"))
MESSAGE_LIMIT               = int(os.getenv("MESSAGE_LIMIT", "18"))
SAFE_SAME_BOT_QUEUE         = os.getenv("SAFE_SAME_BOT_QUEUE", "false").lower() == "true"
ALLOW_UNMATCHED_CONCURRENT  = os.getenv("ALLOW_UNMATCHED_CONCURRENT", "true").lower() == "true"
USE_EVENT_LISTENER          = os.getenv("USE_EVENT_LISTENER", "true").lower() == "true"
USE_POLLING_FALLBACK        = os.getenv("USE_POLLING_FALLBACK", "true").lower() == "true"
KEEPALIVE_INTERVAL          = int(os.getenv("KEEPALIVE_INTERVAL", "300"))

SPECIAL_BOT    = "@faultyhhbot"
BHAGATFLIX_BOT = "@bhagatflix"

# ---- Bhagatflix ----
SUPABASE_URL        = os.getenv("SUPABASE_URL", "https://arjzgyadqemequykgvcz.supabase.co")
SUPABASE_ANON_KEY   = os.getenv(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFyanpneWFkcWVtZXF1eWtndmN6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTI1MDUzNDIsImV4cCI6MjA2ODA4MTM0Mn0.72VjrbubOyq0rtGjRAwjixfRtAQQFUJHKpxI6wnh1Tk"
)
SUPABASE_PROJECT_ID = os.getenv("SUPABASE_PROJECT_ID", "arjzgyadqemequykgvcz")
BHAGATFLIX_EMAIL    = os.getenv("BHAGATFLIX_EMAIL", "")
BHAGATFLIX_PASSWORD = os.getenv("BHAGATFLIX_PASSWORD", "")
BHAGATFLIX_BASE     = os.getenv("BHAGATFLIX_BASE", "https://www.bhagatflix.com")
BHAGATFLIX_ENDPOINTS = {
    "code":      "/api/signin-code",
    "household": "/api/household-code",
    "reset":     "/api/reset-link",
}

_bhagat_token_cache: Dict[str, Any] = {
    "access_token": None, "refresh_token": None, "expires_at": 0
}
_bhagat_token_lock = asyncio.Lock()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("otp-server")

app = FastAPI(title="OTP Python Server")

client1 = TelegramClient(
    StringSession(TG_STRING_SESSION_1),
    API_ID_1,
    API_HASH_1,
    connection_retries=5,
    retry_delay=2,
    auto_reconnect=True,
)

client2: Optional[TelegramClient] = None
if API_ID_2 and API_HASH_2 and TG_STRING_SESSION_2:
    client2 = TelegramClient(
        StringSession(TG_STRING_SESSION_2),
        API_ID_2,
        API_HASH_2,
        connection_retries=5,
        retry_delay=2,
        auto_reconnect=True,
    )

CLIENTS: Dict[str, Optional[TelegramClient]] = {
    "account1": client1,
    "account2": client2,
}

semaphore          = asyncio.Semaphore(SEMAPHORE_LIMIT)
entity_cache:      Dict[tuple, Any]           = {}
bot_locks:         Dict[tuple, asyncio.Lock]  = {}
active_by_bot:     Dict[tuple, int]           = defaultdict(int)
pending_requests:  Dict[str, Dict[str, Any]]  = {}
pending_lock       = asyncio.Lock()


def normalize_account_id(account_id: Any) -> str:
    value = str(account_id or "").strip().lower()
    if value in ("account2", "acc2", "2"):
        return "account2"
    return "account1"


def get_client(account_id: str) -> TelegramClient:
    cli = CLIENTS.get(account_id)
    if cli is None:
        raise RuntimeError(f"ระบบยังไม่ได้ตั้งค่า {account_id} กรุณาติดต่อผู้ดูแล")
    return cli


def get_timeout_for_account(account_id: str) -> float:
    if account_id == "account2":
        return TIMEOUT_SECONDS_ACCOUNT2
    return TIMEOUT_SECONDS_ACCOUNT1

# =========================
# MODELS
# =========================
class OtpRequest(BaseModel):
    email:       str
    botUsername: str
    accountId:   str = "account1"
    mode:        str = "fourdigit"

class ButtonRequest(BaseModel):
    email:       str
    botUsername: str
    row:         int = 0
    col:         int = 0
    buttonText:  str = ""
    messageId:   int = 0
    accountId:   str = "account1"
    mode:        str = "fourdigit"

class YopmailRequest(BaseModel):
    email: str
    mode:  str = "household"

# =========================
# STARTUP / SHUTDOWN
# =========================
@app.on_event("startup")
async def startup() -> None:
    if not API_ID_1 or not API_HASH_1 or not TG_STRING_SESSION_1:
        logger.warning("Missing required Telegram env vars for account1")
    await yopmail_startup()

    await connect_telegram(client1, "account1")

    if client2 is not None:
        await connect_telegram(client2, "account2")

    if USE_EVENT_LISTENER:
        try:
            register_event_listener(client1, "account1")
            if client2 is not None:
                register_event_listener(client2, "account2")
        except Exception:
            logger.exception("event listener register failed")

    asyncio.create_task(telegram_keepalive_loop())

    logger.info("server startup complete")

@app.on_event("shutdown")
async def shutdown():
    await yopmail_shutdown()
    for account_id, cli in CLIENTS.items():
        if cli is None:
            continue
        try:
            await cli.disconnect()
        except Exception:
            pass

async def connect_telegram(cli: TelegramClient, account_id: str) -> None:
    try:
        await cli.connect()
        me = await asyncio.wait_for(cli.get_me(), timeout=10.0)
        if me:
            logger.info("[%s] connected as @%s", account_id, getattr(me, "username", "?"))
    except Exception:
        logger.exception("[%s] connect failed", account_id)

async def ensure_client_ready(cli: TelegramClient, account_id: str) -> None:
    if not cli.is_connected():
        try:
            await cli.connect()
        except Exception as exc:
            raise RuntimeError("ระบบยังไม่พร้อมใช้งาน กรุณาติดต่อผู้ดูแล") from exc

    try:
        me = await asyncio.wait_for(cli.get_me(), timeout=8.0)
        if me is None:
            raise RuntimeError("get_me returned None")
    except asyncio.TimeoutError:
        await _force_reconnect(cli, account_id)
    except Exception:
        await _force_reconnect(cli, account_id)

async def _force_reconnect(cli: TelegramClient, account_id: str) -> None:
    try:
        await asyncio.wait_for(cli.disconnect(), timeout=5.0)
    except Exception:
        pass
    await asyncio.sleep(1.5)
    try:
        await cli.connect()
        me = await asyncio.wait_for(cli.get_me(), timeout=10.0)
        if me is None:
            raise RuntimeError("get_me returned None after reconnect")
    except Exception as exc:
        raise RuntimeError("ระบบยังไม่พร้อมใช้งาน กรุณาติดต่อผู้ดูแล") from exc

async def telegram_keepalive_loop() -> None:
    while True:
        await asyncio.sleep(KEEPALIVE_INTERVAL)
        for account_id, cli in CLIENTS.items():
            if cli is None:
                continue
            try:
                if not cli.is_connected():
                    await cli.connect()
                    continue
                me = await asyncio.wait_for(cli.get_me(), timeout=8.0)
                if me is None:
                    raise RuntimeError("get_me returned None")
            except asyncio.TimeoutError:
                try:
                    await _force_reconnect(cli, account_id)
                except Exception:
                    pass
            except asyncio.CancelledError:
                return
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
    accounts_status: Dict[str, Any] = {}

    for account_id, cli in CLIENTS.items():
        if cli is None:
            accounts_status[account_id] = {"configured": False}
            continue
        info = {"configured": True, "connected": False, "authorized": False, "username": None}
        try:
            info["connected"] = cli.is_connected()
            if info["connected"]:
                try:
                    me = await asyncio.wait_for(cli.get_me(), timeout=6.0)
                    if me is not None:
                        info["authorized"] = True
                        info["username"]   = getattr(me, "username", None)
                except Exception:
                    pass
        except Exception:
            pass
        accounts_status[account_id] = info

    overall_ok = any(s.get("authorized") for s in accounts_status.values())

    return {
        "success":         True,
        "status":          "ok" if overall_ok else "not_ready",
        "accounts":        accounts_status,
        "bhagatflixReady": bool(BHAGATFLIX_EMAIL and BHAGATFLIX_PASSWORD),
        "pendingRequests": len(pending_requests),
        "requestId":       request_id,
    }

@app.post("/get-otp")
async def get_otp(data: OtpRequest) -> Dict[str, Any]:
    request_id   = make_request_id()
    email        = clean_email(data.email)
    bot_username = normalize_bot_username(data.botUsername)
    account_id   = normalize_account_id(data.accountId)
    mode         = normalize_mode(data.mode)

    logger.info("get_otp start requestId=%s email=%s system=%s mode=%s",
                request_id, mask_email(email), bot_username, mode)

    if not email:
        return fail("กรุณากรอกอีเมล", request_id)
    if not bot_username:
        return fail("กรุณาเลือกระบบ", request_id)
    if not is_valid_email(email):
        return fail("รูปแบบอีเมลไม่ถูกต้อง", request_id)

    if is_bhagatflix(bot_username):
        return {
            "success":    False,
            "needButton": True,
            "message":    "กรุณาเลือกเมนูที่ต้องการ",
            "buttons": [
                {"text": "ขอโค้ดเข้าสู่ระบบ",  "row": 0, "col": 0},
                {"text": "ยืนยันครัวเรือน",      "row": 0, "col": 1},
                {"text": "ลิงก์รีเซ็ตรหัสผ่าน",  "row": 0, "col": 2},
            ],
            "messageId":   0,
            "specialMode": True,
            "magicWindow": True,
            "requestId":   request_id,
        }

    try:
        cli = get_client(account_id)
    except RuntimeError as exc:
        return fail(str(exc), request_id)

    async with semaphore:
        async with active_bot_request(account_id, bot_username):
            try:
                await ensure_client_ready(cli, account_id)

                if should_use_special_bot(bot_username):
                    return {
                        "success":    False,
                        "needButton": True,
                        "message":    "กรุณาเลือกเมนูที่ต้องการ",
                        "buttons": [
                            {"text": "ขอโค้ดเข้าสู่ระบบ",  "row": 0, "col": 0},
                            {"text": "ยืนยันครัวเรือน",     "row": 0, "col": 1},
                            {"text": "ลิงก์รีเซ็ตรหัสผ่าน", "row": 0, "col": 2},
                            {"text": "Code 6 หลัก",          "row": 0, "col": 3},
                        ],
                        "messageId":   0,
                        "specialMode": True,
                        "requestId":   request_id,
                    }

                target = await get_cached_entity(cli, account_id, bot_username)
                async with optional_bot_lock(account_id, bot_username):
                    sent_msg = await cli.send_message(target, email)

                return await wait_for_buttons_or_result(
                    cli=cli, account_id=account_id,
                    target=target, bot_username=bot_username, after_id=sent_msg.id,
                    email=email, selected_button="ขอโค้ดเข้าสู่ระบบ",
                    request_id=request_id, expect_buttons=True, special_mode=False,
                )

            except Exception as exc:
                logger.exception("get_otp error")
                return fail(sanitize_error(exc), request_id)

@app.post("/click-button")
async def click_button(data: ButtonRequest) -> Dict[str, Any]:
    request_id   = make_request_id()
    email        = clean_email(data.email)
    bot_username = normalize_bot_username(data.botUsername)
    button_text  = clean_text(data.buttonText)
    account_id   = normalize_account_id(data.accountId)
    mode         = normalize_mode(data.mode)

    if not email:
        return fail("กรุณากรอกอีเมล", request_id)
    if not bot_username:
        return fail("กรุณาเลือกระบบ", request_id)
    if not is_valid_email(email):
        return fail("รูปแบบอีเมลไม่ถูกต้อง", request_id)

    if is_bhagatflix(bot_username):
        return await handle_bhagatflix_click(
            email=email, row=data.row, col=data.col,
            button_text=button_text, request_id=request_id,
        )

    try:
        cli = get_client(account_id)
    except RuntimeError as exc:
        return fail(str(exc), request_id)

    async with semaphore:
        async with active_bot_request(account_id, bot_username):
            try:
                await ensure_client_ready(cli, account_id)
                target = await get_cached_entity(cli, account_id, bot_username)

                if should_use_special_bot(bot_username):
                    command_text = build_special_command(
                        button_text=button_text, email=email,
                        row=data.row, col=data.col,
                    )
                    if not command_text:
                        return fail("ไม่รู้จักเมนูที่เลือก กรุณาลองใหม่อีกครั้ง", request_id)
                    async with optional_bot_lock(account_id, bot_username):
                        sent_msg = await cli.send_message(target, command_text)
                    return await wait_for_buttons_or_result(
                        cli=cli, account_id=account_id,
                        target=target, bot_username=bot_username, after_id=sent_msg.id,
                        email=email,
                        selected_button=button_text or special_title_from_position(data.row, data.col),
                        request_id=request_id, expect_buttons=False, special_mode=True,
                    )

                target_msg = await find_button_message(cli=cli, target=target,
                                                      message_id=data.messageId, email=email)
                if not target_msg:
                    return fail("ไม่พบเมนู กรุณาลองใหม่อีกครั้ง", request_id)

                clicked = await click_target_button(
                    msg=target_msg, row=data.row, col=data.col, button_text=button_text,
                )
                if not clicked:
                    return fail("กดเมนูไม่สำเร็จ กรุณาลองใหม่อีกครั้ง", request_id)

                first_result = await wait_for_buttons_or_result(
                    cli=cli, account_id=account_id,
                    target=target, bot_username=bot_username, after_id=target_msg.id,
                    email=email, selected_button=button_text,
                    request_id=request_id, expect_buttons=False, special_mode=False,
                )

                if first_result and first_result.get("needButton") and first_result.get("buttons"):
                    nested_buttons = first_result.get("buttons") or []
                    if nested_buttons:
                        first_btn     = nested_buttons[0]
                        nested_msg_id = first_result.get("messageId") or 0
                        nested_msg    = await find_button_message(cli=cli, target=target,
                                                                  message_id=nested_msg_id, email=email)
                        if nested_msg:
                            ok = await click_target_button(
                                msg=nested_msg,
                                row=int(first_btn.get("row") or 0),
                                col=int(first_btn.get("col") or 0),
                                button_text=str(first_btn.get("text") or ""),
                            )
                            if ok:
                                return await wait_for_buttons_or_result(
                                    cli=cli, account_id=account_id,
                                    target=target, bot_username=bot_username,
                                    after_id=nested_msg.id, email=email,
                                    selected_button=button_text,
                                    request_id=request_id,
                                    expect_buttons=False, special_mode=False,
                                )

                return first_result

            except Exception as exc:
                logger.exception("click_button error")
                return fail(sanitize_error(exc), request_id)

# =========================
# YOPMAIL (unchanged)
# =========================
class _YopmailIdParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids: List[str] = []
        self._seen: Set[str] = set()
        self._blacklist = {
            "mail", "inbox", "page", "login", "spam",
            "ctrl", "true", "false", "undefined", "null",
        }

    def handle_starttag(self, tag: str, attrs: list) -> None:
        attr_dict = dict(attrs)
        for key in ("id", "data-id"):
            val = attr_dict.get(key, "")
            if val and len(val) >= 3 and val not in self._seen and val.lower() not in self._blacklist:
                self._seen.add(val)
                self.ids.append(val)
        href = attr_dict.get("href", "")
        if href:
            m = re.search(r"[?&]id=([^&\"'\\s]{3,})", href)
            if m:
                val = m.group(1)
                if val not in self._seen and val.lower() not in self._blacklist:
                    self._seen.add(val)
                    self.ids.append(val)
        onclick = attr_dict.get("onclick", "")
        if onclick:
            m = re.search(r"readMail\(['\"]([^'\"]{3,})['\"]\)", onclick)
            if m:
                val = m.group(1)
                if val not in self._seen and val.lower() not in self._blacklist:
                    self._seen.add(val)
                    self.ids.append(val)

def _parse_yopmail_ids(html_text: str) -> List[str]:
    parser = _YopmailIdParser()
    try:
        parser.feed(html_text)
    except Exception:
        pass
    return parser.ids[:5]

def _extract_yopmail_subject(html_text: str) -> str:
    m = re.search(r'<div[^>]*class="[^"]*ellipsis[^"]*"[^>]*>([^<]+)</div>', html_text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r'<title>([^<]+)</title>', html_text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""

def _extract_yopmail_body(html_text: str) -> str:
    m = re.search(r'<div[^>]*id="mail"[^>]*>([\s\S]*?)</div>', html_text, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'<div[^>]*class="[^"]*mail[^"]*"[^>]*>([\s\S]*?)</div>', html_text, re.IGNORECASE)
    if m:
        return m.group(1)
    return html_text

@app.post("/get-yopmail")
async def get_yopmail(data: YopmailRequest) -> Dict[str, Any]:
    request_id = make_request_id()
    email      = clean_email(data.email)
    shortname  = email.split("@")[0]

    if not shortname:
        return fail("รูปแบบอีเมลไม่ถูกต้อง", request_id)

    base_headers = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://yopmail.com/",
    }

    try:
        async with httpx.AsyncClient(
            timeout=25.0,
            follow_redirects=True,
            headers=base_headers,
        ) as http:

            wm_url = f"https://yopmail.com/wm?login={urllib.parse.quote(shortname)}"
            wm_res = await http.get(wm_url)

            yj_token = ""
            m = re.search(r'var\s+yjToken\s*=\s*["\']([^"\']+)["\']', wm_res.text, re.IGNORECASE)
            if not m:
                m = re.search(r'[?&]yj=([a-zA-Z0-9]+)', wm_res.text)
            if m:
                yj_token = m.group(1)

            inbox_url = (
                f"https://yopmail.com/en/inbox"
                f"?login={urllib.parse.quote(shortname)}"
                f"&p=1&d=&ctrl=&scrl=&spam=true"
                f"&yj={urllib.parse.quote(yj_token)}&v=10.0"
            )
            inbox_res = await http.get(
                inbox_url,
                headers={**base_headers, "Referer": str(wm_res.url)},
            )

            ids = _parse_yopmail_ids(inbox_res.text)

            if not ids:
                fb_url = (
                    f"https://yopmail.com/inbox"
                    f"?login={urllib.parse.quote(shortname)}"
                    f"&p=1&yj={urllib.parse.quote(yj_token)}"
                )
                fb_res = await http.get(fb_url, headers=base_headers)
                ids = _parse_yopmail_ids(fb_res.text)

            if not ids:
                ids = _parse_yopmail_ids(wm_res.text)

            if not ids:
                return fail("ไม่พบอีเมลใน Yopmail กรุณาลองใหม่อีกครั้ง", request_id)

            emails_found: List[Dict[str, Any]] = []
            for i, mid in enumerate(ids[:5]):
                mail_url = (
                    f"https://yopmail.com/en/mail"
                    f"?b={urllib.parse.quote(shortname)}&id={urllib.parse.quote(mid)}"
                )
                mail_res = await http.get(
                    mail_url,
                    headers={**base_headers, "Referer": str(inbox_res.url)},
                )

                if mail_res.status_code < 200 or mail_res.status_code >= 300 or not mail_res.text:
                    mail_url2 = (
                        f"https://yopmail.com/mail"
                        f"?b={urllib.parse.quote(shortname)}&id={urllib.parse.quote(mid)}"
                    )
                    mail_res = await http.get(mail_url2, headers=base_headers)

                if mail_res.status_code >= 200 and mail_res.status_code < 300 and mail_res.text:
                    subject      = _extract_yopmail_subject(mail_res.text)
                    html_content = _extract_yopmail_body(mail_res.text)
                    emails_found.append({
                        "id":           mid,
                        "to":           email,
                        "from":         "",
                        "subject":      subject,
                        "html":         html_content,
                        "internalDate": int(time.time() * 1000) - (i * 60000),
                    })

                await asyncio.sleep(0.3)

            if not emails_found:
                return fail("ไม่พบเนื้อหาอีเมลใน Yopmail", request_id)

            return {
                "success":   True,
                "emails":    emails_found,
                "requestId": request_id,
            }

    except Exception as exc:
        logger.exception("[yopmail] error")
        return fail("ระบบไม่ตอบสนอง กรุณาลองใหม่อีกครั้ง", request_id)

# =========================
# BHAGATFLIX
# =========================
def is_bhagatflix(bot_username: str) -> bool:
    return normalize_bot_username(bot_username) == BHAGATFLIX_BOT

def bhagatflix_action_from_position(row: int, col: int, button_text: str) -> Optional[str]:
    text = clean_text(button_text).lower()
    if text:
        if is_code_choice(text):      return "code"
        if is_household_choice(text): return "household"
        if is_reset_choice(text):     return "reset"
    if row == 0 and col == 0: return "code"
    if row == 0 and col == 1: return "household"
    if row == 0 and col == 2: return "reset"
    return None

def bhagatflix_title(action: str) -> str:
    return {
        "code":      "ขอโค้ดเข้าสู่ระบบ",
        "household": "ยืนยันครัวเรือน",
        "reset":     "ลิงก์รีเซ็ตรหัสผ่าน",
    }.get(action, "ข้อมูล")

async def get_bhagatflix_token() -> Optional[Dict[str, Any]]:
    if not BHAGATFLIX_EMAIL or not BHAGATFLIX_PASSWORD:
        return None
    async with _bhagat_token_lock:
        now    = time.time()
        cached = _bhagat_token_cache
        if cached.get("access_token") and cached.get("expires_at", 0) > now + 30:
            return cached
        url = f"{SUPABASE_URL}/auth/v1/token?grant_type=password"
        headers = {
            "Content-Type": "application/json",
            "apikey":       SUPABASE_ANON_KEY,
            "Origin":       BHAGATFLIX_BASE,
            "Referer":      BHAGATFLIX_BASE + "/",
        }
        payload = {"email": BHAGATFLIX_EMAIL, "password": BHAGATFLIX_PASSWORD}
        try:
            async with httpx.AsyncClient(timeout=15.0) as http:
                resp = await http.post(url, json=payload, headers=headers)
            if resp.status_code != 200:
                return None
            data       = resp.json()
            expires_in = int(data.get("expires_in") or 3600)
            cached.update({
                "access_token":  data.get("access_token"),
                "refresh_token": data.get("refresh_token"),
                "expires_at":    now + expires_in,
                "user":          data.get("user"),
                "expires_in":    expires_in,
                "token_type":    data.get("token_type", "bearer"),
            })
            return cached
        except Exception:
            return None

def build_bhagatflix_cookies(token_data: Dict[str, Any]) -> Dict[str, str]:
    cookie_obj = {
        "access_token":  token_data.get("access_token"),
        "refresh_token": token_data.get("refresh_token"),
        "expires_in":    token_data.get("expires_in", 3600),
        "expires_at":    int(token_data.get("expires_at", 0)),
        "token_type":    token_data.get("token_type", "bearer"),
        "user":          token_data.get("user"),
    }
    cookie_json    = json.dumps(cookie_obj, separators=(",", ":"))
    cookie_encoded = urllib.parse.quote(cookie_json, safe="")
    cookies        = {f"sb-{SUPABASE_PROJECT_ID}-auth-token": cookie_encoded}
    chunk_size     = 3000
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
    url     = f"{BHAGATFLIX_BASE}{endpoint}"
    cookies = build_bhagatflix_cookies(token_data)
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {token_data['access_token']}",
        "User-Agent":    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Origin":        BHAGATFLIX_BASE,
        "Referer":       BHAGATFLIX_BASE + "/",
        "Accept":        "application/json, text/plain, */*",
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
        return {"ok": False, "error": str(exc)}

def parse_bhagatflix_response(
    action: str, raw: Dict[str, Any], request_id: str, customer_email: str
) -> Dict[str, Any]:
    title = bhagatflix_title(action)
    if not raw.get("ok"):
        msg  = "ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง"
        data = raw.get("data") or {}
        if isinstance(data, dict):
            err = data.get("error") or data.get("message")
            if err:
                err_lower = str(err).lower()
                if "not authenticated" in err_lower or "unauthorized" in err_lower:
                    msg = "ระบบยังไม่พร้อมใช้งาน กรุณาติดต่อผู้ดูแล"
        return fail(msg, request_id)

    data   = raw.get("data") or {}
    emails = data.get("emails") if isinstance(data, dict) else None

    if isinstance(emails, list) and emails:
        first     = emails[0] or {}
        html_body = first.get("html") or first.get("body") or ""
        subject   = first.get("subject") or title
        from_addr = first.get("from") or first.get("sender") or ""
        date_str  = first.get("date") or first.get("received_at") or ""
        return {
            "success":     True,
            "type":        "email",
            "title":       title,
            "value":       "",
            "message":     subject,
            "subject":     subject,
            "from":        from_addr,
            "date":        date_str,
            "html":        html_body,
            "email":       customer_email,
            "magicWindow": True,
            "requestId":   request_id,
        }

    if isinstance(data, dict):
        html_single    = data.get("html") or data.get("body") or ""
        subject_single = data.get("subject") or title
        if html_single:
            return {
                "success":     True,
                "type":        "email",
                "title":       title,
                "value":       "",
                "message":     subject_single,
                "subject":     subject_single,
                "from":        data.get("from") or "",
                "date":        data.get("date") or "",
                "html":        html_single,
                "email":       customer_email,
                "magicWindow": True,
                "requestId":   request_id,
            }

    return fail("ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง", request_id)

async def handle_bhagatflix_click(
    email: str, row: int, col: int, button_text: str, request_id: str
) -> Dict[str, Any]:
    action = bhagatflix_action_from_position(row, col, button_text)
    if not action:
        return fail("ไม่รู้จักเมนูที่เลือก กรุณาลองใหม่อีกครั้ง", request_id)
    raw = await call_bhagatflix_api(action, email)
    return parse_bhagatflix_response(action, raw, request_id, email)

# =========================
# EVENT LISTENER
# =========================
def register_event_listener(cli: TelegramClient, account_id: str) -> None:
    @cli.on(events.NewMessage(incoming=True))
    async def on_new_message(event: Any) -> None:
        try:
            msg = event.message
            if not msg:
                return
            await dispatch_incoming_message(msg, account_id)
        except Exception:
            logger.exception("[%s] event listener failed", account_id)

async def dispatch_incoming_message(msg: Any, account_id: str) -> None:
    async with pending_lock:
        if not pending_requests:
            return
        matched_keys: List[str] = []
        for key, pending in list(pending_requests.items()):
            if pending.get("done"):
                continue
            if pending.get("account_id") != account_id:
                continue
            after_id = int(pending.get("after_id") or 0)
            if msg.id <= after_id:
                continue
            result = build_result_from_message(
                msg=msg, bot_username=pending["bot_username"], email=pending["email"],
                selected_button=pending["selected_button"], request_id=pending["request_id"],
                expect_buttons=pending["expect_buttons"], special_mode=pending["special_mode"],
            )
            if not result:
                continue
            if not is_relevant_message(
                msg=msg, account_id=account_id,
                bot_username=pending["bot_username"], email=pending["email"],
                selected_button=pending["selected_button"], special_mode=pending["special_mode"],
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
    cli: TelegramClient, account_id: str,
    target: Any, bot_username: str, after_id: int, email: str,
    selected_button: str, request_id: str, expect_buttons: bool, special_mode: bool,
) -> Dict[str, Any]:
    if USE_EVENT_LISTENER:
        result = await wait_with_event_listener(
            cli=cli, account_id=account_id,
            target=target, bot_username=bot_username, after_id=after_id, email=email,
            selected_button=selected_button, request_id=request_id,
            expect_buttons=expect_buttons, special_mode=special_mode,
        )
        if result:
            return result
    return await wait_with_polling(
        cli=cli, account_id=account_id,
        target=target, bot_username=bot_username, after_id=after_id, email=email,
        selected_button=selected_button, request_id=request_id,
        expect_buttons=expect_buttons, special_mode=special_mode,
    )

async def wait_with_event_listener(
    cli: TelegramClient, account_id: str,
    target: Any, bot_username: str, after_id: int, email: str,
    selected_button: str, request_id: str, expect_buttons: bool, special_mode: bool,
) -> Optional[Dict[str, Any]]:
    loop    = asyncio.get_event_loop()
    future: asyncio.Future = loop.create_future()
    pending_key = request_id
    target_id   = get_entity_identity(target)

    async with pending_lock:
        pending_requests[pending_key] = {
            "future":          future,
            "request_id":      request_id,
            "account_id":      account_id,
            "bot_username":    bot_username,
            "target_id":       target_id,
            "after_id":        after_id,
            "email":           email,
            "selected_button": selected_button,
            "expect_buttons":  expect_buttons,
            "special_mode":    special_mode,
            "created_at":      time.time(),
            "done":            False,
        }

    polling_task: Optional[asyncio.Task] = None
    if USE_POLLING_FALLBACK:
        polling_task = asyncio.create_task(
            polling_fallback_to_future(
                future=future, cli=cli, account_id=account_id,
                target=target, bot_username=bot_username,
                after_id=after_id, email=email, selected_button=selected_button,
                request_id=request_id, expect_buttons=expect_buttons, special_mode=special_mode,
            )
        )

    try:
        timeout = get_timeout_for_account(account_id)
        return await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        return fail("ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง", request_id)
    finally:
        async with pending_lock:
            pending_requests.pop(pending_key, None)
        if polling_task and not polling_task.done():
            polling_task.cancel()

async def polling_fallback_to_future(
    future: asyncio.Future, cli: TelegramClient, account_id: str,
    target: Any, bot_username: str, after_id: int,
    email: str, selected_button: str, request_id: str,
    expect_buttons: bool, special_mode: bool,
) -> None:
    try:
        start_time = asyncio.get_event_loop().time()
        timeout    = get_timeout_for_account(account_id)
        while True:
            if future.done():
                return
            if asyncio.get_event_loop().time() - start_time > timeout:
                return
            messages = await get_new_messages(cli, target, after_id)
            for msg in messages:
                if future.done():
                    return
                result = build_result_from_message(
                    msg=msg, bot_username=bot_username, email=email,
                    selected_button=selected_button, request_id=request_id,
                    expect_buttons=expect_buttons, special_mode=special_mode,
                )
                if not result:
                    continue
                if not is_relevant_message(
                    msg=msg, account_id=account_id,
                    bot_username=bot_username, email=email,
                    selected_button=selected_button, special_mode=special_mode,
                ):
                    continue
                if not future.done():
                    future.set_result(result)
                return
            await asyncio.sleep(POLL_INTERVAL)
    except asyncio.CancelledError:
        return
    except Exception:
        logger.exception("polling fallback failed")

async def wait_with_polling(
    cli: TelegramClient, account_id: str,
    target: Any, bot_username: str, after_id: int, email: str,
    selected_button: str, request_id: str, expect_buttons: bool, special_mode: bool,
) -> Dict[str, Any]:
    start_time = asyncio.get_event_loop().time()
    timeout    = get_timeout_for_account(account_id)
    while True:
        if asyncio.get_event_loop().time() - start_time > timeout:
            return fail("ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง", request_id)
        messages = await get_new_messages(cli, target, after_id)
        for msg in messages:
            result = build_result_from_message(
                msg=msg, bot_username=bot_username, email=email,
                selected_button=selected_button, request_id=request_id,
                expect_buttons=expect_buttons, special_mode=special_mode,
            )
            if not result:
                continue
            if is_relevant_message(
                msg=msg, account_id=account_id,
                bot_username=bot_username, email=email,
                selected_button=selected_button, special_mode=special_mode,
            ):
                return result
        await asyncio.sleep(POLL_INTERVAL)

def build_result_from_message(
    msg: Any, bot_username: str, email: str, selected_button: str,
    request_id: str, expect_buttons: bool, special_mode: bool,
) -> Optional[Dict[str, Any]]:
    if expect_buttons and getattr(msg, "buttons", None):
        return {
            "success":    False,
            "needButton": True,
            "message":    "กรุณาเลือกเมนูที่ต้องการ",
            "buttons":    extract_buttons(msg),
            "messageId":  msg.id,
            "requestId":  request_id,
        }
    no_data = extract_no_data_message(msg.message or "")
    if no_data:
        return fail(no_data, request_id)
    return extract_code_or_link(msg=msg, selected_button=selected_button, request_id=request_id)

async def get_new_messages(cli: TelegramClient, target: Any, after_id: int) -> List[Any]:
    messages     = await cli.get_messages(target, limit=MESSAGE_LIMIT)
    new_messages = [m for m in messages if m and m.id > after_id]
    new_messages.sort(key=lambda item: item.id)
    return new_messages

# =========================
# TELEGRAM HELPERS
# =========================
async def get_cached_entity(cli: TelegramClient, account_id: str, bot_username: str) -> Any:
    key = (account_id, normalize_bot_username(bot_username))
    if key in entity_cache:
        return entity_cache[key]
    entity            = await cli.get_entity(key[1])
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
async def active_bot_request(account_id: str, bot_username: str) -> AsyncGenerator[None, None]:
    key = (account_id, normalize_bot_username(bot_username))
    active_by_bot[key] += 1
    try:
        yield
    finally:
        active_by_bot[key] = max(0, active_by_bot[key] - 1)

@asynccontextmanager
async def optional_bot_lock(account_id: str, bot_username: str) -> AsyncGenerator[None, None]:
    key = (account_id, normalize_bot_username(bot_username))
    if not SAFE_SAME_BOT_QUEUE:
        yield
        return
    if key not in bot_locks:
        bot_locks[key] = asyncio.Lock()
    async with bot_locks[key]:
        yield

async def find_button_message(cli: TelegramClient, target: Any,
                              message_id: int = 0, email: str = "") -> Optional[Any]:
    if message_id:
        try:
            msg = await cli.get_messages(target, ids=message_id)
            if msg and getattr(msg, "buttons", None):
                return msg
        except Exception:
            pass
    messages     = await cli.get_messages(target, limit=MESSAGE_LIMIT)
    email_lower  = clean_email(email)
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
                    if current_text == button_text or button_text in current_text or current_text in button_text:
                        await msg.click(row_index, col_index)
                        return True
        await msg.click(row, col)
        return True
    except Exception:
        return False

# =========================
# MATCHING / EXTRACTION
# =========================
def is_relevant_message(
    msg: Any, account_id: str, bot_username: str, email: str,
    selected_button: str, special_mode: bool = False
) -> bool:
    text         = html.unescape(msg.message or "")
    text_lower   = clean_text(text).lower()
    email_lower  = clean_email(email)
    bot_key      = (account_id, normalize_bot_username(bot_username))
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
            if any(k in text_lower for k in ("travel verify", "household", "travel", "verify")):
                return ALLOW_UNMATCHED_CONCURRENT
        if is_sixdigit_choice(selected_lower):
            if looks_like_code_message(text_lower):
                return ALLOW_UNMATCHED_CONCURRENT
        if is_code_choice(selected_lower):
            if looks_like_code_message(text_lower):
                return ALLOW_UNMATCHED_CONCURRENT

    if special_mode and ALLOW_UNMATCHED_CONCURRENT:
        return True

    return False

def extract_code_or_link(msg: Any, selected_button: str, request_id: str) -> Optional[Dict[str, Any]]:
    text           = html.unescape(msg.message or "")
    selected_lower = clean_text(selected_button).lower()

    is_reset_request     = any(k in selected_lower for k in ("reset", "รีเซ็ต", "pwlink", "password", "ลิงก์"))
    is_household_request = any(k in selected_lower for k in ("ครัวเรือน", "household", "travel", "verify"))

    if is_reset_request:
        urls      = extract_urls_from_message(msg)
        reset_url = pick_reset_url(urls, text)
        if reset_url:
            return _link_result(selected_button, text, reset_url, request_id)
        if urls:
            return _link_result(selected_button, text, urls[0], request_id)
        return None

    if is_household_request:
        urls       = extract_urls_from_message(msg)
        text_lower = clean_text(text).lower()

        household_url = pick_household_url(urls, text)
        if household_url:
            return _link_result(selected_button, text, household_url, request_id)

        household_code = extract_household_code(text)
        if household_code:
            return _code_result(selected_button, text, household_code, request_id)

        link_hints = ("click here", "verify here", "this link", "ลิงก์", "กดลิงก์", "คลิก")
        if urls and any(h in text_lower for h in link_hints):
            return _link_result(selected_button, text, urls[0], request_id)

        if urls:
            netflix_url = next((u for u in urls if "netflix" in u.lower() or "nflxext" in u.lower()), None)
            if netflix_url and not is_footer_url(netflix_url):
                return _link_result(selected_button, text, netflix_url, request_id)

        code = extract_code(text)
        if code:
            return _code_result(selected_button, text, code, request_id)

        if urls:
            return _link_result(selected_button, text, urls[0], request_id)

        return None

    code = extract_code(text)
    if code:
        return _code_result(selected_button, text, code, request_id)

    urls = extract_urls_from_message(msg)
    if urls:
        return _link_result(selected_button, text, urls[0], request_id)

    return None

def _code_result(selected_button: str, text: str, value: str, request_id: str) -> Dict[str, Any]:
    return {
        "success":   True,
        "type":      "code",
        "title":     selected_button or detect_title_from_text(text),
        "value":     value,
        "message":   text,
        "requestId": request_id,
    }

def _link_result(selected_button: str, text: str, value: str, request_id: str) -> Dict[str, Any]:
    return {
        "success":   True,
        "type":      "link",
        "title":     selected_button or detect_title_from_text(text),
        "value":     value,
        "message":   text,
        "requestId": request_id,
    }

def is_footer_url(url: str) -> bool:
    skip = (
        "help.netflix", "termsofuse", "privacypolicy", "notificationsettings",
        "url_terms", "url_privacy", "url_help", "url_corp_info", "url_logo",
        "url_email", "url_src", "url_comm_settings", "beaconimages",
    )
    u = url.lower()
    return any(s in u for s in skip)

def pick_household_url(urls: List[str], text: str) -> Optional[str]:
    if not urls:
        return None
    priority_keywords = ("travel", "verify", "household", "account/access", "accountaccess", "lkid=url_cta", "/p/", "nftoken")
    for url in urls:
        if is_footer_url(url):
            continue
        u = url.lower()
        if any(k in u for k in priority_keywords):
            return url
    for url in urls:
        if is_footer_url(url):
            continue
        u = url.lower()
        if "netflix.com" in u or "nflxext" in u:
            return url
    return None

def extract_household_code(text: str) -> Optional[str]:
    if not text:
        return None
    cleaned  = html.unescape(text)
    patterns = [
        r"travel\s*verify\s*code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"household\s*code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"Netflix\s*Travel\s*Verify\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"ยืนยัน\s*ครัวเรือน\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"รหัส\s*ยืนยัน\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"verification\s*code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
    ]
    for pattern in patterns:
        m = re.search(pattern, cleaned, re.IGNORECASE)
        if m:
            code = extract_first_4_digit_code(m.group(1)) or extract_first_4_to_8_digit_code(m.group(1))
            if code:
                return code
    return None

def pick_reset_url(urls: List[str], text: str) -> Optional[str]:
    if not urls:
        return None
    priority_keywords = ("password", "reset", "nftoken", "lkid=url_cta", "/p/", "/account")
    for url in urls:
        u = url.lower()
        if any(k in u for k in priority_keywords):
            return url
    for url in urls:
        u = url.lower()
        if is_footer_url(url):
            continue
        if "netflix.com" in u or "nflxext" in u:
            return url
    return None

def extract_code(text: str) -> Optional[str]:
    if not text:
        return None
    text = html.unescape(text)
    specific_patterns = [
        r"Netflix\s*Sign[-\s]*in\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        r"Netflix\s*Travel\s*Verify\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
    ]
    for pattern in specific_patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            code = extract_first_4_digit_code(m.group(1))
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
        r"ยืนยัน\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
    ]
    for pattern in label_patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            code = extract_first_4_digit_code(m.group(1)) or extract_first_4_to_8_digit_code(m.group(1))
            if code:
                return code
    if looks_like_code_message(text):
        code = extract_first_4_digit_code(text) or extract_first_4_to_8_digit_code(text)
        if code:
            return code
    return None

def extract_first_4_digit_code(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"(?<!\d)([0-9]{4})(?!\d)", text)
    return m.group(1) if m else None

def extract_first_4_to_8_digit_code(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"(?<!\d)([0-9]{4,8})(?!\d)", text)
    return m.group(1) if m else None

def extract_no_data_message(text: str) -> Optional[str]:
    value = clean_text(html.unescape(text)).lower()
    no_data_patterns = (
        "no new emails found", "no matching email found", "no new data found",
        "new data failed", "not found", "no data", "no email",
        "ไม่พบข้อมูล", "ไม่พบอีเมล",
    )
    if any(p in value for p in no_data_patterns):
        return "ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง"
    return None

def extract_urls_from_message(msg: Any) -> List[str]:
    urls: List[str] = []
    text     = html.unescape(msg.message or "")
    entities = getattr(msg, "entities", None) or []

    for entity in entities:
        if isinstance(entity, MessageEntityTextUrl):
            url = getattr(entity, "url", None)
            if url:
                urls.append(url)
        elif isinstance(entity, MessageEntityUrl):
            try:
                raw_url = text[entity.offset: entity.offset + entity.length]
                if raw_url:
                    urls.append(raw_url)
            except Exception:
                pass

    for url in re.findall(r"https?://[^\s<>\]\)\"']+", text, re.IGNORECASE):
        urls.append(url)

    if getattr(msg, "buttons", None):
        for row in msg.buttons:
            for button in row:
                url = getattr(button, "url", None)
                if url:
                    urls.append(url)

    return unique_list(clean_url(url) for url in urls if url)

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
    value    = clean_text(text).lower()
    keywords = (
        "code", "otp", "verification", "verify", "login", "signin", "sign in", "sign-in",
        "travel verify", "netflix sign-in code", "netflix travel verify code",
        "เข้าสู่ระบบ", "รหัส", "โค้ด", "ยืนยัน",
    )
    return any(k in value for k in keywords)

def extract_buttons(message: Any) -> List[Dict[str, Any]]:
    buttons: List[Dict[str, Any]] = []
    for row_index, row in enumerate(message.buttons or []):
        for col_index, button in enumerate(row):
            buttons.append({
                "text": clean_text(getattr(button, "text", "")),
                "row":  row_index,
                "col":  col_index,
            })
    return buttons

register_yopmail_routes(app, extract_code)

# =========================
# SPECIAL BOT (@FaultyHHBot) — 4 commands
# =========================
def build_special_command(button_text: str, email: str, row: int = 0, col: int = 0) -> Optional[str]:
    """
    @FaultyHHBot รองรับ 4 คำสั่ง:
    - col 0 → /code  (signin code 4 หลัก)
    - col 1 → /link  (household)
    - col 2 → /pwlink (reset password)
    - col 3 → /verif (verification code 6 หลัก)
    """
    text = clean_text(button_text).lower()
    if not text:
        if row == 0 and col == 0: return f"/code {email}"
        if row == 0 and col == 1: return f"/link {email}"
        if row == 0 and col == 2: return f"/pwlink {email}"
        if row == 0 and col == 3: return f"/verif {email}"
    # ⚠️ ต้องเช็ค sixdigit ก่อน code เพราะ keyword "code" ซ้ำกับ "code 6"
    if is_sixdigit_choice(text):  return f"/verif {email}"
    if is_code_choice(text):      return f"/code {email}"
    if is_household_choice(text): return f"/link {email}"
    if is_reset_choice(text):     return f"/pwlink {email}"
    return None

def special_title_from_position(row: int, col: int) -> str:
    if row == 0 and col == 0: return "ขอโค้ดเข้าสู่ระบบ"
    if row == 0 and col == 1: return "ยืนยันครัวเรือน"
    if row == 0 and col == 2: return "ลิงก์รีเซ็ตรหัสผ่าน"
    if row == 0 and col == 3: return "Code 6 หลัก"
    return "ข้อมูล"

def is_sixdigit_choice(text: str) -> bool:
    """จับปุ่ม Code 6 หลัก — ต้องเช็คก่อน is_code_choice"""
    value = clean_text(text).lower()
    return any(k in value for k in (
        "6 หลัก", "6หลัก", "6 digit", "6digit",
        "six digit", "sixdigit", "six-digit",
        "/verif", "verif"
    ))

def is_code_choice(text: str) -> bool:
    value = clean_text(text).lower()
    return any(k in value for k in ("เข้าสู่ระบบ", "โค้ด", "code", "signin", "sign in", "sign-in"))

def is_household_choice(text: str) -> bool:
    value = clean_text(text).lower()
    return any(k in value for k in ("ครัวเรือน", "household", "travel", "verify"))

def is_reset_choice(text: str) -> bool:
    value = clean_text(text).lower()
    return any(k in value for k in ("รีเซ็ต", "reset", "pwlink", "password"))

def normalize_bot_username(bot_username: Any) -> str:
    value = clean_text(bot_username).lower()
    if not value:
        return ""
    if not value.startswith("@"):
        value = "@" + value
    return value

def should_use_special_bot(bot_username: str) -> bool:
    return normalize_bot_username(bot_username) == SPECIAL_BOT

def normalize_mode(mode: Any) -> str:
    value = str(mode or "").strip().lower()
    if value in ("sixdigit", "6digit", "six", "6"): return "sixdigit"
    if value in ("fourdigit", "4digit", "four", "4", "signin", "login"): return "fourdigit"
    if value in ("reset", "password", "forgot", "resetlink", "reset-link"): return "reset"
    if value in ("household", "travel"): return "household"
    return "fourdigit"

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
    masked_name  = (name[0:1] + "***") if len(name) <= 2 else (name[:2] + "***" + name[-1:])
    return masked_name + "@" + domain

def clean_text(value: Any) -> str:
    return str(value or "").strip()

def clean_email(value: Any) -> str:
    return clean_text(value).replace(" ", "").lower()

def clean_url(url: str) -> str:
    return clean_text(url).rstrip(".,;)]}")

def unique_list(items: Any) -> List[str]:
    seen:   Set[str]  = set()
    result: List[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result

def sanitize_error(error: Any) -> str:
    raw = clean_text(error)
    replacements = (
        (r"telegram",      "ระบบ"),
        (r"telethon",      "ระบบ"),
        (r"botusername",   "ระบบ"),
        (r"\bbot\b",       "ระบบ"),
        (r"maker",         "ระบบ"),
        (r"stringsession", "ระบบ"),
        (r"api_id",        "ระบบ"),
        (r"api_hash",      "ระบบ"),
        (r"traceback",     "ระบบ"),
        (r"exception",     "ระบบ"),
        (r"supabase",      "ระบบ"),
        (r"bhagatflix",    "ระบบ"),
        (r"faultyhh",      "ระบบ"),
    )
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
        "success":   False,
        "message":   sanitize_error(message),
        "requestId": request_id or make_request_id(),
    }

import os
import re
import uuid
import asyncio
import logging
from fastapi import FastAPI
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
TG_STRING_SESSION = os.getenv("TG_STRING_SESSION", "")

TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "35"))
FALLBACK_GRACE_SECONDS = float(os.getenv("FALLBACK_GRACE_SECONDS", "2"))
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "0.5"))
MESSAGE_FETCH_LIMIT = int(os.getenv("MESSAGE_FETCH_LIMIT", "15"))
MAX_CONCURRENT_TELEGRAM_ACTIONS = int(os.getenv("MAX_CONCURRENT_TELEGRAM_ACTIONS", "5"))

SPECIAL_BOT = "@faultyhhbot"

app = FastAPI()
client = TelegramClient(StringSession(TG_STRING_SESSION), API_ID, API_HASH)
telegram_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TELEGRAM_ACTIONS)


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
    specialMode: bool = False


@app.on_event("startup")
async def startup():
    if not API_ID or not API_HASH or not TG_STRING_SESSION:
        logging.warning("Telegram environment variables are missing")

    await client.connect()
    logging.info("Telegram client connected")


@app.on_event("shutdown")
async def shutdown():
    await client.disconnect()
    logging.info("Telegram client disconnected")


@app.get("/")
async def home():
    return {
        "status": "running",
        "telegramConnected": client.is_connected()
    }


@app.get("/health")
async def health():
    authorized = False

    try:
        authorized = await client.is_user_authorized()
    except Exception:
        authorized = False

    return {
        "success": True,
        "status": "running",
        "telegramConnected": client.is_connected(),
        "authorized": authorized,
        "timeoutSeconds": TIMEOUT_SECONDS,
        "fallbackGraceSeconds": FALLBACK_GRACE_SECONDS,
        "pollIntervalSeconds": POLL_INTERVAL_SECONDS,
        "messageFetchLimit": MESSAGE_FETCH_LIMIT,
        "maxConcurrentActions": MAX_CONCURRENT_TELEGRAM_ACTIONS
    }


@app.post("/get-otp")
async def get_otp(data: OtpRequest):
    request_id = make_request_id()

    email = clean_text(data.email)
    bot_username = normalize_bot_username(data.botUsername)

    if not email:
        return fail("กรุณากรอกอีเมลก่อนดำเนินการ", request_id)

    if not bot_username:
        return fail("ระบบยังไม่พบรายการที่ต้องการ กรุณาลองใหม่อีกครั้ง", request_id)

    if not await client.is_user_authorized():
        return fail("ระบบขัดข้องชั่วคราว กรุณาลองใหม่อีกครั้ง", request_id)

    async with telegram_semaphore:
        try:
            logging.info(f"[{request_id}] get-otp | bot={bot_username} | email={mask_email(email)}")

            bot = await client.get_entity(bot_username)

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
                    "botUsername": SPECIAL_BOT,
                    "requestId": request_id
                }

            sent_msg = await client.send_message(bot, email)

            result = await wait_for_buttons_or_result(
                bot=bot,
                after_id=sent_msg.id,
                email=email,
                request_id=request_id
            )

            return attach_request_id(result, request_id)

        except Exception:
            logging.exception(f"[{request_id}] get-otp error")
            return fail("ระบบขัดข้องชั่วคราว กรุณาลองใหม่อีกครั้ง", request_id)


@app.post("/click-button")
async def click_button(data: ButtonRequest):
    request_id = make_request_id()

    email = clean_text(data.email)
    bot_username = normalize_bot_username(data.botUsername)
    button_text = clean_text(data.buttonText)

    if not email:
        return fail("กรุณากรอกอีเมลก่อนดำเนินการ", request_id)

    if not bot_username:
        return fail("ระบบยังไม่พบรายการที่ต้องการ กรุณาลองใหม่อีกครั้ง", request_id)

    if not await client.is_user_authorized():
        return fail("ระบบขัดข้องชั่วคราว กรุณาลองใหม่อีกครั้ง", request_id)

    async with telegram_semaphore:
        try:
            logging.info(
                f"[{request_id}] click-button | bot={bot_username} | email={mask_email(email)} | "
                f"row={data.row} | col={data.col} | buttonText={button_text} | "
                f"messageId={data.messageId} | specialMode={data.specialMode}"
            )

            use_special = (
                should_use_special_bot(bot_username)
                or data.specialMode is True
                or is_special_button_action(
                    button_text=button_text,
                    row=data.row,
                    col=data.col,
                    message_id=data.messageId
                )
            )

            if use_special:
                bot = await client.get_entity(SPECIAL_BOT)

                command_text = build_faulty_command(
                    button_text=button_text,
                    email=email,
                    row=data.row,
                    col=data.col
                )

                if not command_text:
                    return fail("ระบบยังไม่พบเมนูที่ต้องการ กรุณาลองใหม่อีกครั้ง", request_id)

                sent_msg = await client.send_message(bot, command_text)

                result = await wait_for_faulty_result(
                    bot=bot,
                    after_id=sent_msg.id,
                    selected_button=button_text,
                    email=email,
                    request_id=request_id
                )

                return attach_request_id(result, request_id)

            bot = await client.get_entity(bot_username)

            target_msg = await find_button_message(
                bot=bot,
                message_id=data.messageId,
                email=email
            )

            if not target_msg:
                return fail("ระบบยังไม่พบเมนูที่ต้องการ กรุณาลองใหม่อีกครั้ง", request_id)

            clicked = await click_target_button(
                msg=target_msg,
                row=data.row,
                col=data.col,
                button_text=button_text
            )

            if not clicked:
                return fail("ระบบยังไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง", request_id)

            result = await wait_for_normal_result(
                bot=bot,
                after_id=target_msg.id,
                selected_button=button_text,
                email=email,
                request_id=request_id
            )

            return attach_request_id(result, request_id)

        except Exception:
            logging.exception(f"[{request_id}] click-button error")
            return fail("ระบบขัดข้องชั่วคราว กรุณาลองใหม่อีกครั้ง", request_id)


async def wait_for_buttons_or_result(bot, after_id, email, request_id=""):
    start_time = asyncio.get_event_loop().time()
    fallback_button_msg = None
    fallback_result = None
    fallback_found_time = None

    while True:
        now = asyncio.get_event_loop().time()

        if now - start_time > TIMEOUT_SECONDS:
            if fallback_result:
                return fallback_result

            if fallback_button_msg:
                return build_button_response(fallback_button_msg)

            return fail("ระบบยังไม่พบข้อมูลในรอบนี้ กรุณาลองใหม่อีกครั้ง")

        if fallback_result and fallback_found_time:
            if now - fallback_found_time >= FALLBACK_GRACE_SECONDS:
                return fallback_result

        if fallback_button_msg and fallback_found_time:
            if now - fallback_found_time >= FALLBACK_GRACE_SECONDS:
                return build_button_response(fallback_button_msg)

        messages = await client.get_messages(bot, limit=MESSAGE_FETCH_LIMIT)
        new_messages = [m for m in messages if m.id > after_id]
        new_messages.sort(key=lambda x: x.id)

        for msg in new_messages:
            text = msg.message or ""

            if msg.buttons:
                if email.lower() in text.lower():
                    return build_button_response(msg)

                if fallback_button_msg is None:
                    fallback_button_msg = msg
                    fallback_found_time = now

            result = extract_normal_code_or_link(msg, "ขอโค้ดเข้าสู่ระบบ")

            if result:
                if email.lower() in text.lower():
                    return result

                if fallback_result is None:
                    fallback_result = result
                    fallback_found_time = now

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def wait_for_normal_result(bot, after_id, selected_button, email, request_id=""):
    start_time = asyncio.get_event_loop().time()
    fallback_result = None
    fallback_found_time = None

    while True:
        now = asyncio.get_event_loop().time()

        if now - start_time > TIMEOUT_SECONDS:
            if fallback_result:
                return fallback_result

            return fail("ระบบใช้เวลานานเกินไป กรุณาลองใหม่อีกครั้ง")

        if fallback_result and fallback_found_time:
            if now - fallback_found_time >= FALLBACK_GRACE_SECONDS:
                return fallback_result

        messages = await client.get_messages(bot, limit=MESSAGE_FETCH_LIMIT)
        new_messages = [m for m in messages if m.id > after_id]
        new_messages.sort(key=lambda x: x.id)

        for msg in new_messages:
            result = extract_normal_code_or_link(msg, selected_button)

            if not result:
                continue

            text = msg.message or ""

            if email.lower() in text.lower():
                return result

            if fallback_result is None:
                fallback_result = result
                fallback_found_time = now

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def wait_for_faulty_result(bot, after_id, selected_button, email, request_id=""):
    start_time = asyncio.get_event_loop().time()
    fallback_result = None
    fallback_found_time = None

    while True:
        now = asyncio.get_event_loop().time()

        if now - start_time > TIMEOUT_SECONDS:
            if fallback_result:
                return fallback_result

            return fail("ระบบยังไม่พบโค้ดหรือลิงก์ในรอบนี้ กรุณาลองใหม่อีกครั้ง")

        if fallback_result and fallback_found_time:
            if now - fallback_found_time >= FALLBACK_GRACE_SECONDS:
                return fallback_result

        messages = await client.get_messages(bot, limit=MESSAGE_FETCH_LIMIT)
        new_messages = [m for m in messages if m.id > after_id]
        new_messages.sort(key=lambda x: x.id)

        for msg in new_messages:
            result = extract_faulty_result(msg, selected_button)

            if not result:
                continue

            text = msg.message or ""

            if email.lower() in text.lower():
                return result

            if fallback_result is None:
                fallback_result = result
                fallback_found_time = now

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def find_button_message(bot, message_id=0, email=""):
    if message_id:
        try:
            msg = await client.get_messages(bot, ids=message_id)

            if msg and getattr(msg, "buttons", None):
                return msg
        except Exception:
            pass

    messages = await client.get_messages(bot, limit=MESSAGE_FETCH_LIMIT)

    email_lower = email.lower().strip()
    fallback_msg = None

    for msg in messages:
        if not getattr(msg, "buttons", None):
            continue

        text = (msg.message or "").lower()

        if email_lower and email_lower in text:
            return msg

        if fallback_msg is None:
            fallback_msg = msg

    return fallback_msg


async def click_target_button(msg, row=0, col=0, button_text=""):
    button_text = clean_text(button_text).lower()

    if button_text:
        for row_index, button_row in enumerate(msg.buttons or []):
            for col_index, button in enumerate(button_row):
                current_text = clean_text(button.text).lower()

                if (
                    current_text == button_text
                    or button_text in current_text
                    or current_text in button_text
                ):
                    await msg.click(row_index, col_index)
                    return True

    try:
        await msg.click(row, col)
        return True
    except Exception:
        return False


def extract_faulty_result(msg, selected_button):
    text = msg.message or ""

    otp_match = re.search(r"OTP Code:\s*([0-9]{4,8})", text, re.IGNORECASE)
    if otp_match:
        return success_code(otp_match.group(1), selected_button, text)

    code_match = re.search(r"\b([0-9]{4,8})\b", text)
    if code_match and looks_like_code_message(text):
        return success_code(code_match.group(1), selected_button, text)

    hidden_urls = extract_hidden_urls_from_message(msg)
    if hidden_urls:
        return success_link(hidden_urls[-1], selected_button, text)

    link_match = re.search(r"Link:\s*(https?://[^\s]+)", text, re.IGNORECASE)
    if link_match:
        return success_link(link_match.group(1), selected_button, text)

    raw_link_match = re.search(r"(https?://[^\s]+)", text, re.IGNORECASE)
    if raw_link_match:
        return success_link(raw_link_match.group(1), selected_button, text)

    return None


def extract_normal_code_or_link(msg, selected_button):
    text = msg.message or ""

    code_match = re.search(r"Code:\s*([0-9]{4,8})", text, re.IGNORECASE)
    if code_match:
        return success_code(code_match.group(1), selected_button, text)

    otp_match = re.search(r"OTP Code:\s*([0-9]{4,8})", text, re.IGNORECASE)
    if otp_match:
        return success_code(otp_match.group(1), selected_button, text)

    any_code_match = re.search(r"\b([0-9]{4,8})\b", text)
    if any_code_match and looks_like_code_message(text):
        return success_code(any_code_match.group(1), selected_button, text)

    hidden_urls = extract_hidden_urls_from_message(msg)
    if hidden_urls:
        return success_link(hidden_urls[-1], selected_button, text)

    link_match = re.search(r"Link:\s*(https?://[^\s]+)", text, re.IGNORECASE)
    if link_match:
        return success_link(link_match.group(1), selected_button, text)

    raw_link_match = re.search(r"(https?://[^\s]+)", text, re.IGNORECASE)
    if raw_link_match:
        return success_link(raw_link_match.group(1), selected_button, text)

    return None


def extract_hidden_urls_from_message(msg):
    urls = []

    entities = getattr(msg, "entities", None) or []
    text = msg.message or ""

    for entity in entities:
        if isinstance(entity, MessageEntityTextUrl):
            if getattr(entity, "url", None):
                urls.append(entity.url)

        elif isinstance(entity, MessageEntityUrl):
            try:
                start = entity.offset
                end = entity.offset + entity.length
                raw_url = text[start:end]

                if raw_url:
                    urls.append(raw_url)
            except Exception:
                pass

    if getattr(msg, "buttons", None):
        for row in msg.buttons:
            for button in row:
                btn_url = getattr(button, "url", None)

                if btn_url:
                    urls.append(btn_url)

    unique_urls = []
    seen = set()

    for url in urls:
        if url and url not in seen:
            seen.add(url)
            unique_urls.append(url)

    return unique_urls


def build_faulty_command(button_text, email, row=0, col=0):
    t = clean_text(button_text).lower()

    if not t:
        if row == 0 and col == 0:
            return f"/code {email}"
        if row == 0 and col == 1:
            return f"/link {email}"
        if row == 0 and col == 2:
            return f"/pwlink {email}"

    if "เข้าสู่ระบบ" in t or "โค้ด" in t or "signin" in t or "code" in t:
        return f"/code {email}"

    if "ครัวเรือน" in t or "household" in t or "link" in t:
        return f"/link {email}"

    if "รีเซ็ต" in t or "รีเซ็ตรหัสผ่าน" in t or "reset" in t or "pwlink" in t:
        return f"/pwlink {email}"

    return None


def is_special_button_action(button_text, row=0, col=0, message_id=0):
    t = clean_text(button_text).lower()

    special_keywords = [
        "ขอโค้ดเข้าสู่ระบบ",
        "โค้ด",
        "เข้าสู่ระบบ",
        "ยืนยันครัวเรือน",
        "ครัวเรือน",
        "ลิงก์รีเซ็ตรหัสผ่าน",
        "รีเซ็ต",
        "reset",
        "code",
        "signin",
        "household",
        "pwlink"
    ]

    if any(keyword in t for keyword in special_keywords):
        return True

    if message_id == 0 and row == 0 and col in [0, 1, 2]:
        return True

    return False


def looks_like_code_message(text):
    t = (text or "").lower()

    keywords = [
        "code",
        "otp",
        "verification",
        "verify",
        "login",
        "signin",
        "sign in",
        "เข้าสู่ระบบ",
        "รหัส",
        "โค้ด",
        "ยืนยัน"
    ]

    return any(keyword in t for keyword in keywords)


def normalize_bot_username(bot_username):
    bot_username = clean_text(bot_username).lower()

    if not bot_username:
        return ""

    if not bot_username.startswith("@"):
        bot_username = "@" + bot_username

    return bot_username


def should_use_special_bot(bot_username):
    return normalize_bot_username(bot_username) == SPECIAL_BOT


def build_button_response(msg):
    return {
        "success": False,
        "needButton": True,
        "message": "กรุณาเลือกเมนูที่ต้องการ",
        "buttons": extract_buttons(msg),
        "messageId": msg.id
    }


def extract_buttons(message):
    buttons = []

    for row_index, row in enumerate(message.buttons or []):
        for col_index, button in enumerate(row):
            buttons.append({
                "text": button.text or "",
                "row": row_index,
                "col": col_index
            })

    return buttons


def success_code(value, title, message):
    return {
        "success": True,
        "type": "code",
        "title": title or "โค้ดของคุณ",
        "value": value,
        "message": message
    }


def success_link(value, title, message):
    return {
        "success": True,
        "type": "link",
        "title": title or "ลิงก์ของคุณ",
        "value": value,
        "message": message
    }


def fail(message, request_id=""):
    result = {
        "success": False,
        "message": message
    }

    if request_id:
        result["requestId"] = request_id

    return result


def attach_request_id(result, request_id):
    if isinstance(result, dict):
        result.setdefault("requestId", request_id)

    return result


def make_request_id():
    return uuid.uuid4().hex[:10]


def clean_text(value):
    return str(value or "").strip()


def mask_email(email):
    email = email or ""

    if "@" not in email:
        return "***"

    name, domain = email.split("@", 1)

    if len(name) <= 2:
        masked_name = name[:1] + "***"
    else:
        masked_name = name[:2] + "***"

    return masked_name + "@" + domain

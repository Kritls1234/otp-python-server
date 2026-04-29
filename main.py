import os
import re
import uuid
import html
import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl


API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
TG_STRING_SESSION = os.getenv("TG_STRING_SESSION", "")

TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "60"))
SEMAPHORE_LIMIT = int(os.getenv("SEMAPHORE_LIMIT", "5"))

SPECIAL_BOT = "@faultyhhbot"

app = FastAPI(title="OTP Python Server")

client = TelegramClient(
    StringSession(TG_STRING_SESSION),
    API_ID,
    API_HASH
)

semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("otp-server")


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


@app.on_event("startup")
async def startup() -> None:
    if not API_ID or not API_HASH or not TG_STRING_SESSION:
        logger.warning("Missing required environment variables")

    await client.connect()


@app.on_event("shutdown")
async def shutdown() -> None:
    await client.disconnect()


@app.get("/")
async def home() -> Dict[str, Any]:
    return {
        "success": True,
        "status": "running"
    }


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
            "requestId": request_id
        }

    except Exception as exc:
        logger.exception("health failed requestId=%s", request_id)

        return {
            "success": False,
            "status": "error",
            "message": sanitize_error(exc),
            "requestId": request_id
        }


@app.post("/get-otp")
async def get_otp(data: OtpRequest) -> Dict[str, Any]:
    request_id = make_request_id()
    email = clean_text(data.email)
    bot_username = clean_text(data.botUsername)

    logger.info(
        "get_otp start requestId=%s email=%s system=%s",
        request_id,
        mask_email(email),
        normalize_bot_username(bot_username)
    )

    if not email:
        return fail("กรุณากรอกอีเมล", request_id)

    if not bot_username:
        return fail("กรุณาเลือกระบบ", request_id)

    if not is_valid_email(email):
        return fail("รูปแบบอีเมลไม่ถูกต้อง", request_id)

    async with semaphore:
        try:
            if not client.is_connected():
                await client.connect()

            if not await client.is_user_authorized():
                return fail("ระบบยังไม่พร้อมใช้งาน กรุณาติดต่อผู้ดูแล", request_id)

            if should_use_special_bot(bot_username):
                return {
                    "success": False,
                    "needButton": True,
                    "message": "กรุณาเลือกเมนูที่ต้องการ",
                    "buttons": [
                        {
                            "text": "ขอโค้ดเข้าสู่ระบบ",
                            "row": 0,
                            "col": 0
                        },
                        {
                            "text": "ยืนยันครัวเรือน",
                            "row": 0,
                            "col": 1
                        },
                        {
                            "text": "ลิงก์รีเซ็ตรหัสผ่าน",
                            "row": 0,
                            "col": 2
                        }
                    ],
                    "messageId": 0,
                    "specialMode": True,
                    "requestId": request_id
                }

            target = await client.get_entity(bot_username)
            sent_msg = await client.send_message(target, email)

            result = await wait_for_buttons_or_result(
                target=target,
                after_id=sent_msg.id,
                email=email,
                selected_button="ขอโค้ดเข้าสู่ระบบ",
                request_id=request_id
            )

            logger.info(
                "get_otp done requestId=%s email=%s success=%s",
                request_id,
                mask_email(email),
                result.get("success")
            )

            return result

        except Exception as exc:
            logger.exception(
                "get_otp error requestId=%s email=%s",
                request_id,
                mask_email(email)
            )

            return fail(sanitize_error(exc), request_id)


@app.post("/click-button")
async def click_button(data: ButtonRequest) -> Dict[str, Any]:
    request_id = make_request_id()
    email = clean_text(data.email)
    bot_username = clean_text(data.botUsername)
    button_text = clean_text(data.buttonText)

    logger.info(
        "click_button start requestId=%s email=%s system=%s row=%s col=%s messageId=%s",
        request_id,
        mask_email(email),
        normalize_bot_username(bot_username),
        data.row,
        data.col,
        data.messageId
    )

    if not email:
        return fail("กรุณากรอกอีเมล", request_id)

    if not bot_username:
        return fail("กรุณาเลือกระบบ", request_id)

    if not is_valid_email(email):
        return fail("รูปแบบอีเมลไม่ถูกต้อง", request_id)

    async with semaphore:
        try:
            if not client.is_connected():
                await client.connect()

            if not await client.is_user_authorized():
                return fail("ระบบยังไม่พร้อมใช้งาน กรุณาติดต่อผู้ดูแล", request_id)

            target = await client.get_entity(bot_username)

            if should_use_special_bot(bot_username):
                command_text = build_special_command(
                    button_text=button_text,
                    email=email,
                    row=data.row,
                    col=data.col
                )

                if not command_text:
                    return fail("ไม่รู้จักเมนูที่เลือก กรุณาลองใหม่อีกครั้ง", request_id)

                sent_msg = await client.send_message(target, command_text)

                result = await wait_for_result(
                    target=target,
                    after_id=sent_msg.id,
                    email=email,
                    selected_button=button_text or special_title_from_position(data.row, data.col),
                    request_id=request_id,
                    special_mode=True
                )

                logger.info(
                    "click_button special done requestId=%s email=%s success=%s",
                    request_id,
                    mask_email(email),
                    result.get("success")
                )

                return result

            target_msg = await find_button_message(
                target=target,
                message_id=data.messageId,
                email=email
            )

            if not target_msg:
                return fail("ไม่พบเมนู กรุณาลองใหม่อีกครั้ง", request_id)

            clicked = await click_target_button(
                msg=target_msg,
                row=data.row,
                col=data.col,
                button_text=button_text
            )

            if not clicked:
                return fail("กดเมนูไม่สำเร็จ กรุณาลองใหม่อีกครั้ง", request_id)

            result = await wait_for_result(
                target=target,
                after_id=target_msg.id,
                email=email,
                selected_button=button_text,
                request_id=request_id,
                special_mode=False
            )

            logger.info(
                "click_button normal done requestId=%s email=%s success=%s",
                request_id,
                mask_email(email),
                result.get("success")
            )

            return result

        except Exception as exc:
            logger.exception(
                "click_button error requestId=%s email=%s",
                request_id,
                mask_email(email)
            )

            return fail(sanitize_error(exc), request_id)


async def wait_for_buttons_or_result(
    target: Any,
    after_id: int,
    email: str,
    selected_button: str,
    request_id: str
) -> Dict[str, Any]:
    start_time = asyncio.get_event_loop().time()

    while True:
        elapsed = asyncio.get_event_loop().time() - start_time

        if elapsed > TIMEOUT_SECONDS:
            return fail("ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง", request_id)

        messages = await client.get_messages(target, limit=35)
        new_messages = [m for m in messages if m.id > after_id]
        new_messages.sort(key=lambda item: item.id)

        for msg in new_messages:
            if getattr(msg, "buttons", None):
                return {
                    "success": False,
                    "needButton": True,
                    "message": "กรุณาเลือกเมนูที่ต้องการ",
                    "buttons": extract_buttons(msg),
                    "messageId": msg.id,
                    "requestId": request_id
                }

            result = extract_code_or_link(
                msg=msg,
                selected_button=selected_button,
                request_id=request_id
            )

            if result:
                return result

        await asyncio.sleep(1)


async def wait_for_result(
    target: Any,
    after_id: int,
    email: str,
    selected_button: str,
    request_id: str,
    special_mode: bool = False
) -> Dict[str, Any]:
    start_time = asyncio.get_event_loop().time()

    while True:
        elapsed = asyncio.get_event_loop().time() - start_time

        if elapsed > TIMEOUT_SECONDS:
            return fail("ไม่พบข้อมูล กรุณาลองใหม่อีกครั้ง", request_id)

        messages = await client.get_messages(target, limit=35)
        new_messages = [m for m in messages if m.id > after_id]
        new_messages.sort(key=lambda item: item.id)

        for msg in new_messages:
            result = extract_code_or_link(
                msg=msg,
                selected_button=selected_button,
                request_id=request_id
            )

            if result:
                return result

        await asyncio.sleep(1)


async def find_button_message(
    target: Any,
    message_id: int = 0,
    email: str = ""
) -> Optional[Any]:
    if message_id:
        try:
            msg = await client.get_messages(target, ids=message_id)

            if msg and getattr(msg, "buttons", None):
                return msg
        except Exception:
            pass

    messages = await client.get_messages(target, limit=35)

    email_lower = clean_text(email).lower()
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


async def click_target_button(
    msg: Any,
    row: int = 0,
    col: int = 0,
    button_text: str = ""
) -> bool:
    button_text = clean_text(button_text).lower()

    try:
        if button_text:
            for row_index, button_row in enumerate(msg.buttons or []):
                for col_index, button in enumerate(button_row):
                    current_text = clean_text(getattr(button, "text", "")).lower()

                    if (
                        current_text == button_text
                        or button_text in current_text
                        or current_text in button_text
                    ):
                        await msg.click(row_index, col_index)
                        return True

        await msg.click(row, col)
        return True

    except Exception:
        return False


def extract_code_or_link(
    msg: Any,
    selected_button: str,
    request_id: str
) -> Optional[Dict[str, Any]]:
    text = msg.message or ""
    text = html.unescape(text)

    code = extract_code(text)

    if code:
        return {
            "success": True,
            "type": "code",
            "title": selected_button or detect_title_from_text(text),
            "value": code,
            "message": text,
            "requestId": request_id
        }

    urls = extract_urls_from_message(msg)

    if urls:
        selected_url = select_url_for_result(
            urls=urls,
            text=text,
            selected_button=selected_button
        )

        return {
            "success": True,
            "type": "link",
            "title": selected_button or detect_title_from_text(text),
            "value": selected_url,
            "message": text,
            "requestId": request_id
        }

    return None


def extract_code(text: str) -> Optional[str]:
    if not text:
        return None

    text = html.unescape(text)

    signin_match = re.search(
        r"Netflix\s*Sign[-\s]*in\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        text,
        re.IGNORECASE
    )

    if signin_match:
        code = extract_first_4_digit_code(signin_match.group(1))
        if code:
            return code

    travel_match = re.search(
        r"Netflix\s*Travel\s*Verify\s*Code\s*[:：]?\s*([\s\S]*?)(?:Account\s*Country|🌍|$)",
        text,
        re.IGNORECASE
    )

    if travel_match:
        code = extract_first_4_digit_code(travel_match.group(1))
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

    if match:
        return match.group(1)

    return None


def extract_first_4_to_8_digit_code(text: str) -> Optional[str]:
    if not text:
        return None

    match = re.search(r"(?<!\d)([0-9]{4,8})(?!\d)", text)

    if match:
        return match.group(1)

    return None


def select_url_for_result(
    urls: List[str],
    text: str,
    selected_button: str
) -> str:
    if not urls:
        return ""

    selected_text = clean_text(selected_button).lower()
    body_text = clean_text(text).lower()

    if (
        "reset" in selected_text
        or "password" in selected_text
        or "รีเซ็ต" in selected_text
        or "รีเซ็ตรหัสผ่าน" in selected_text
        or "password reset" in body_text
        or "reset link" in body_text
        or "netflix password reset link" in body_text
    ):
        return urls[0]

    return urls[0]


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
        "code",
        "otp",
        "verification",
        "verify",
        "login",
        "signin",
        "sign in",
        "sign-in",
        "travel verify",
        "netflix sign-in code",
        "netflix travel verify code",
        "เข้าสู่ระบบ",
        "รหัส",
        "โค้ด",
        "ยืนยัน"
    ]

    return any(keyword in value for keyword in keywords)


def extract_buttons(message: Any) -> List[Dict[str, Any]]:
    buttons: List[Dict[str, Any]] = []

    for row_index, row in enumerate(message.buttons or []):
        for col_index, button in enumerate(row):
            buttons.append({
                "text": clean_text(getattr(button, "text", "")),
                "row": row_index,
                "col": col_index
            })

    return buttons


def build_special_command(
    button_text: str,
    email: str,
    row: int = 0,
    col: int = 0
) -> Optional[str]:
    text = clean_text(button_text).lower()

    if not text:
        if row == 0 and col == 0:
            return f"/code {email}"

        if row == 0 and col == 1:
            return f"/link {email}"

        if row == 0 and col == 2:
            return f"/pwlink {email}"

    if (
        "เข้าสู่ระบบ" in text
        or "โค้ด" in text
        or "code" in text
        or "signin" in text
        or "sign in" in text
        or "sign-in" in text
    ):
        return f"/code {email}"

    if (
        "ครัวเรือน" in text
        or "household" in text
        or "travel" in text
        or "verify" in text
        or "link" in text
    ):
        return f"/link {email}"

    if (
        "รีเซ็ต" in text
        or "รีเซ็ตรหัสผ่าน" in text
        or "reset" in text
        or "pwlink" in text
        or "password" in text
    ):
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


def normalize_bot_username(bot_username: str) -> str:
    value = clean_text(bot_username).lower()

    if not value:
        return ""

    if not value.startswith("@"):
        value = "@" + value

    return value


def should_use_special_bot(bot_username: str) -> bool:
    return normalize_bot_username(bot_username) == SPECIAL_BOT


def is_valid_email(email: str) -> bool:
    return bool(re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email))


def make_request_id() -> str:
    return uuid.uuid4().hex[:12]


def mask_email(email: str) -> str:
    email = clean_text(email)

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


def clean_url(url: str) -> str:
    return clean_text(url).rstrip(".,;)]}")


def unique_list(items: Any) -> List[str]:
    seen = set()
    result: List[str] = []

    for item in items:
        if item and item not in seen:
            seen.add(item)
            result.append(item)

    return result


def sanitize_error(error: Any) -> str:
    raw = clean_text(error)

    replacements = [
        (r"telegram", "ระบบ"),
        (r"telethon", "ระบบ"),
        (r"botusername", "ระบบ"),
        (r"bot", "ระบบ"),
        (r"maker", "ระบบ"),
        (r"stringsession", "ระบบ"),
        (r"api_id", "ระบบ"),
        (r"api_hash", "ระบบ"),
        (r"traceback", "ระบบ"),
        (r"exception", "ระบบ")
    ]

    for pattern, repl in replacements:
        raw = re.sub(pattern, repl, raw, flags=re.IGNORECASE)

    if not raw or len(raw) > 180:
        return "เกิดข้อผิดพลาด กรุณาลองใหม่อีกครั้ง"

    return raw


def fail(message: str, request_id: str = "") -> Dict[str, Any]:
    return {
        "success": False,
        "message": sanitize_error(message),
        "requestId": request_id or make_request_id()
    }

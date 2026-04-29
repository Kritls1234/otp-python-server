import os
import re
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl

API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
TG_STRING_SESSION = os.getenv("TG_STRING_SESSION", "")

TIMEOUT_SECONDS = 60
MAX_RETRY = 1
SPECIAL_BOT = "@faultyhhbot"

app = FastAPI()
client = TelegramClient(StringSession(TG_STRING_SESSION), API_ID, API_HASH)


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
async def startup():
    await client.connect()


@app.get("/")
async def home():
    return {"status": "running"}


def should_use_special_bot(bot_username: str) -> bool:
    return bot_username.strip().lower() == SPECIAL_BOT


@app.post("/get-otp")
async def get_otp(data: OtpRequest):
    email = data.email.strip()
    bot_username = data.botUsername.strip()

    if not email:
        return fail("ไม่มีอีเมล")
    if not bot_username:
        return fail("ไม่มี BotUsername")
    if not await client.is_user_authorized():
        return fail("Telegram ยังไม่ได้ล็อกอิน")

    try:
        bot = await client.get_entity(bot_username)

        if should_use_special_bot(bot_username):
            return {
                "success": False,
                "needButton": True,
                "message": "กรุณาเลือกบริการที่ต้องการ",
                "buttons": [
                    {"text": "ขอโค้ดเข้าสู่ระบบ", "row": 0, "col": 0},
                    {"text": "ยืนยันครัวเรือน", "row": 0, "col": 1},
                    {"text": "ลิงก์รีเซ็ตรหัสผ่าน", "row": 0, "col": 2}
                ],
                "messageId": 0,
                "specialMode": True
            }

        return await run_normal_get_otp_with_retry(
            bot=bot,
            email=email
        )

    except Exception as e:
        return fail(str(e))


@app.post("/click-button")
async def click_button(data: ButtonRequest):
    email = data.email.strip()
    bot_username = data.botUsername.strip()
    button_text = (data.buttonText or "").strip()

    if not email:
        return fail("ไม่มีอีเมล")
    if not bot_username:
        return fail("ไม่มี BotUsername")
    if not await client.is_user_authorized():
        return fail("Telegram ยังไม่ได้ล็อกอิน")

    try:
        bot = await client.get_entity(bot_username)

        if should_use_special_bot(bot_username):
            command_text = build_faulty_command(button_text, email)

            if not command_text:
                return fail("ไม่รู้จักคำสั่งที่เลือก")

            return await run_special_command_with_retry(
                bot=bot,
                command_text=command_text,
                selected_button=button_text,
                email=email
            )

        target_msg = await find_button_message(
            bot=bot,
            message_id=data.messageId,
            email=email
        )

        if not target_msg:
            return fail("ไม่พบปุ่มจากบอท กรุณาขอโค้ดใหม่อีกครั้ง")

        return await run_normal_click_with_retry(
            bot=bot,
            target_msg=target_msg,
            row=data.row,
            col=data.col,
            button_text=button_text,
            email=email
        )

    except Exception as e:
        return fail(str(e))


async def run_normal_get_otp_with_retry(bot, email):
    last_result = None

    for attempt in range(MAX_RETRY + 1):
        sent_msg = await client.send_message(bot, email)

        result = await wait_for_buttons_or_result(
            bot=bot,
            after_id=sent_msg.id,
            email=email
        )

        if not is_retryable_fail(result):
            return result

        last_result = result

        if attempt < MAX_RETRY:
            await asyncio.sleep(2)

    return last_result or fail("บอทไม่ส่งข้อมูลกลับมา")


async def run_special_command_with_retry(bot, command_text, selected_button, email):
    last_result = None

    for attempt in range(MAX_RETRY + 1):
        sent_msg = await client.send_message(bot, command_text)

        result = await wait_for_faulty_result(
            bot=bot,
            after_id=sent_msg.id,
            selected_button=selected_button,
            email=email
        )

        if not is_retryable_fail(result):
            return result

        last_result = result

        if attempt < MAX_RETRY:
            await asyncio.sleep(2)

    return last_result or fail("บอทไม่ส่งข้อมูลกลับมา")


async def run_normal_click_with_retry(bot, target_msg, row, col, button_text, email):
    last_result = None

    for attempt in range(MAX_RETRY + 1):
        if attempt == 0:
            clicked = await click_target_button(
                msg=target_msg,
                row=row,
                col=col,
                button_text=button_text
            )

            if not clicked:
                return fail("กดปุ่มไม่สำเร็จ กรุณาตรวจ row/col หรือชื่อปุ่ม")

            after_id = target_msg.id

        else:
            sent_msg = await client.send_message(bot, email)

            button_result = await wait_for_buttons_or_result(
                bot=bot,
                after_id=sent_msg.id,
                email=email
            )

            if button_result.get("success") is True:
                return button_result

            if not button_result.get("needButton"):
                last_result = button_result
                continue

            retry_message_id = button_result.get("messageId", 0)

            retry_msg = await find_button_message(
                bot=bot,
                message_id=retry_message_id,
                email=email
            )

            if not retry_msg:
                last_result = fail("ไม่พบปุ่มจากบอทหลัง retry")
                continue

            clicked = await click_target_button(
                msg=retry_msg,
                row=row,
                col=col,
                button_text=button_text
            )

            if not clicked:
                last_result = fail("กดปุ่มหลัง retry ไม่สำเร็จ")
                continue

            after_id = retry_msg.id

        result = await wait_for_normal_result(
            bot=bot,
            after_id=after_id,
            selected_button=button_text,
            email=email
        )

        if not is_retryable_fail(result):
            return result

        last_result = result

        if attempt < MAX_RETRY:
            await asyncio.sleep(2)

    return last_result or fail("บอท Maker ไม่ส่งข้อมูลกลับมา")


async def find_button_message(bot, message_id: int = 0, email: str = ""):
    if message_id:
        try:
            msg = await client.get_messages(bot, ids=message_id)
            if msg and getattr(msg, "buttons", None):
                return msg
        except Exception:
            pass

    messages = await client.get_messages(bot, limit=30)

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


async def click_target_button(msg, row: int = 0, col: int = 0, button_text: str = ""):
    button_text = (button_text or "").strip().lower()

    if button_text:
        for row_index, button_row in enumerate(msg.buttons or []):
            for col_index, button in enumerate(button_row):
                current_text = (button.text or "").strip().lower()

                if current_text == button_text or button_text in current_text or current_text in button_text:
                    await msg.click(row_index, col_index)
                    return True

    try:
        await msg.click(row, col)
        return True
    except Exception:
        return False


def build_faulty_command(button_text: str, email: str):
    t = button_text.strip().lower()

    if "เข้าสู่ระบบ" in t or "โค้ด" in t or "signin" in t or "code" in t:
        return f"/code {email}"

    if "ครัวเรือน" in t or "household" in t:
        return f"/link {email}"

    if "รีเซ็ตรหัสผ่าน" in t or "reset" in t or "pwlink" in t:
        return f"/pwlink {email}"

    return None


async def wait_for_buttons_or_result(bot, after_id, email):
    start_time = asyncio.get_event_loop().time()
    fallback_button_msg = None
    fallback_result = None

    while True:
        if asyncio.get_event_loop().time() - start_time > TIMEOUT_SECONDS:
            if fallback_result:
                return fallback_result

            if fallback_button_msg:
                return build_button_response(fallback_button_msg)

            return fail("บอทไม่ส่งข้อมูลกลับมา")

        messages = await client.get_messages(bot, limit=30)
        new_messages = [m for m in messages if m.id > after_id]
        new_messages.sort(key=lambda x: x.id)

        for msg in new_messages:
            text = msg.message or ""

            if msg.buttons:
                if email.lower() in text.lower():
                    return build_button_response(msg)

                if fallback_button_msg is None:
                    fallback_button_msg = msg

            result = extract_normal_code_or_link(msg, "ขอโค้ดเข้าสู่ระบบ")

            if result:
                if email.lower() in text.lower():
                    return result

                if fallback_result is None:
                    fallback_result = result

        await asyncio.sleep(1)


async def wait_for_normal_result(bot, after_id, selected_button, email):
    start_time = asyncio.get_event_loop().time()
    fallback_result = None

    while True:
        if asyncio.get_event_loop().time() - start_time > TIMEOUT_SECONDS:
            if fallback_result:
                return fallback_result

            return fail("บอท Maker ไม่ส่งข้อมูลกลับมา")

        messages = await client.get_messages(bot, limit=30)
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

        await asyncio.sleep(1)


async def wait_for_faulty_result(bot, after_id, selected_button, email):
    start_time = asyncio.get_event_loop().time()
    fallback_result = None

    while True:
        if asyncio.get_event_loop().time() - start_time > TIMEOUT_SECONDS:
            if fallback_result:
                return fallback_result

            return fail("บอทไม่ส่งข้อมูลกลับมา")

        messages = await client.get_messages(bot, limit=30)
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

        await asyncio.sleep(1)


def build_button_response(msg):
    return {
        "success": False,
        "needButton": True,
        "message": "กรุณาเลือกสิ่งที่ต้องการ",
        "buttons": extract_buttons(msg),
        "messageId": msg.id
    }


def is_retryable_fail(result):
    if not isinstance(result, dict):
        return False

    if result.get("success") is True:
        return False

    if result.get("needButton") is True:
        return False

    message = (result.get("message") or "").lower()

    retry_keywords = [
        "ไม่ส่งข้อมูล",
        "ไม่ส่งปุ่ม",
        "ไม่พบปุ่ม",
        "ไม่พบข้อมูล",
        "timeout",
        "timed out"
    ]

    return any(keyword.lower() in message for keyword in retry_keywords)


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

    for u in urls:
        if u and u not in seen:
            seen.add(u)
            unique_urls.append(u)

    return unique_urls


def extract_faulty_result(msg, selected_button):
    text = msg.message or ""

    otp_match = re.search(r"OTP Code:\s*([0-9]{4,8})", text, re.IGNORECASE)
    if otp_match:
        return {
            "success": True,
            "type": "code",
            "title": selected_button or "ขอโค้ดเข้าสู่ระบบ",
            "value": otp_match.group(1),
            "message": text
        }

    hidden_urls = extract_hidden_urls_from_message(msg)
    if hidden_urls:
        return {
            "success": True,
            "type": "link",
            "title": selected_button or "ลิงก์",
            "value": hidden_urls[-1],
            "message": text
        }

    link_match = re.search(r"Link:\s*(https?://[^\s]+)", text, re.IGNORECASE)
    if link_match:
        return {
            "success": True,
            "type": "link",
            "title": selected_button or "ลิงก์",
            "value": link_match.group(1),
            "message": text
        }

    return None


def extract_normal_code_or_link(msg, selected_button):
    text = msg.message or ""

    code_match = re.search(r"Code:\s*([0-9]{4,8})", text, re.IGNORECASE)
    if code_match:
        return {
            "success": True,
            "type": "code",
            "title": selected_button or "ขอโค้ดเข้าสู่ระบบ",
            "value": code_match.group(1),
            "message": text
        }

    hidden_urls = extract_hidden_urls_from_message(msg)
    if hidden_urls:
        return {
            "success": True,
            "type": "link",
            "title": selected_button or "ลิงก์",
            "value": hidden_urls[-1],
            "message": text
        }

    link_match = re.search(r"Link:\s*(https?://[^\s]+)", text, re.IGNORECASE)
    if link_match:
        return {
            "success": True,
            "type": "link",
            "title": selected_button or "ลิงก์",
            "value": link_match.group(1),
            "message": text
        }

    return None


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


def fail(message):
    return {
        "success": False,
        "message": message
    }

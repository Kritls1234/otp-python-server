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
        if bot_username.lower() == SPECIAL_BOT:
            return {
                "success": False,
                "needButton": True,
                "message": "กรุณาเลือกบริการที่ต้องการ",
                "buttons": [
                    {"text": "ขอโค้ดเข้าสู่ระบบ", "row": 0, "col": 0},
                    {"text": "ยืนยันครัวเรือน", "row": 0, "col": 1},
                    {"text": "ลิงก์รีเซ็ตรหัสผ่าน", "row": 0, "col": 2}
                ],
                "messageId": 0
            }

        bot = await client.get_entity(bot_username)
        sent_msg = await client.send_message(bot, email)

        return await wait_for_buttons_only(
            bot=bot,
            after_id=sent_msg.id,
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

        if bot_username.lower() == SPECIAL_BOT:
            command_text = build_faulty_command(button_text, email)

            if not command_text:
                return fail("ไม่รู้จักคำสั่งที่เลือก")

            sent_msg = await client.send_message(bot, command_text)

            return await wait_for_faulty_result(
                bot=bot,
                after_id=sent_msg.id,
                selected_button=button_text,
                email=email
            )

        target_msg = None

        if data.messageId:
            target_msg = await client.get_messages(bot, ids=data.messageId)

        if not target_msg or not getattr(target_msg, "buttons", None):
            return fail("ไม่พบปุ่มจากข้อความเดิม กรุณาขอโค้ดใหม่อีกครั้ง")

        await target_msg.click(data.row, data.col)

        return await wait_for_normal_result(
            bot=bot,
            after_id=target_msg.id,
            selected_button=button_text,
            email=email
        )

    except Exception as e:
        return fail(str(e))


def build_faulty_command(button_text: str, email: str):
    t = button_text.strip().lower()

    if "เข้าสู่ระบบ" in t or "โค้ด" in t or "signin" in t or "code" in t:
        return f"/code {email}"

    if "ครัวเรือน" in t or "household" in t:
        return f"/link {email}"

    if "รีเซ็ตรหัสผ่าน" in t or "reset" in t or "pwlink" in t:
        return f"/pwlink {email}"

    return None


async def wait_for_buttons_only(bot, after_id, email):
    start_time = asyncio.get_event_loop().time()
    fallback_button_msg = None

    while True:
        if asyncio.get_event_loop().time() - start_time > TIMEOUT_SECONDS:
            if fallback_button_msg:
                return build_button_response(fallback_button_msg)

            return fail("บอท Maker ไม่ส่งปุ่มกลับมา")

        messages = await client.get_messages(bot, limit=20)
        new_messages = [m for m in messages if m.id > after_id]
        new_messages.sort(key=lambda x: x.id)

        for msg in new_messages:
            if not msg.buttons:
                continue

            text = msg.message or ""

            if email.lower() in text.lower():
                return build_button_response(msg)

            if fallback_button_msg is None:
                fallback_button_msg = msg

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

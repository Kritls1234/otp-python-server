import os, re, asyncio
from fastapi import FastAPI
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
TG_STRING_SESSION = os.getenv("TG_STRING_SESSION", "")

TIMEOUT_SECONDS = 60
OTP_PATTERN = r"\b\d{4,8}\b"
URL_PATTERN = r"https?://[^\s]+"

app = FastAPI()
client = TelegramClient(StringSession(TG_STRING_SESSION), API_ID, API_HASH)
lock = asyncio.Lock()


class OtpRequest(BaseModel):
    email: str
    botUsername: str


class ButtonRequest(BaseModel):
    email: str
    botUsername: str
    row: int
    col: int


@app.on_event("startup")
async def startup():
    await client.connect()


@app.get("/")
async def home():
    return {"status": "running"}


@app.post("/get-otp")
async def get_otp(data: OtpRequest):
    async with lock:
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
            before_id = await get_latest_message_id(bot)

            await client.send_message(bot, email)

            return await wait_for_new_reply(bot, before_id)

        except Exception as e:
            return fail(str(e))


@app.post("/click-button")
async def click_button(data: ButtonRequest):
    async with lock:
        bot_username = data.botUsername.strip()

        if not bot_username:
            return fail("ไม่มี BotUsername")
        if not await client.is_user_authorized():
            return fail("Telegram ยังไม่ได้ล็อกอิน")

        try:
            bot = await client.get_entity(bot_username)
            before_id = await get_latest_message_id(bot)

            messages = await client.get_messages(bot, limit=10)
            target_msg = None

            for msg in messages:
                if msg.buttons:
                    target_msg = msg
                    break

            if not target_msg:
                return fail("ไม่พบปุ่มจากบอท")

            await target_msg.click(data.row, data.col)

            return await wait_for_new_reply(bot, before_id)

        except Exception as e:
            return fail(str(e))


async def get_latest_message_id(bot):
    messages = await client.get_messages(bot, limit=1)
    return messages[0].id if messages else 0


async def wait_for_new_reply(bot, before_id):
    start_time = asyncio.get_event_loop().time()

    while True:
        if asyncio.get_event_loop().time() - start_time > TIMEOUT_SECONDS:
            return fail("บอท Maker ไม่ตอบกลับภายในเวลาที่กำหนด")

        messages = await client.get_messages(bot, limit=10)
        new_messages = [m for m in messages if m.id > before_id]
        new_messages.sort(key=lambda x: x.id)

        for msg in new_messages:
            text = msg.message or ""

            if msg.buttons:
                return {
                    "success": False,
                    "needButton": True,
                    "message": "กรุณาเลือกบริการที่ต้องการ",
                    "buttons": extract_buttons(msg)
                }

            result = extract_result(text)
            if result:
                return result

        await asyncio.sleep(2)


def extract_result(text):
    urls = re.findall(URL_PATTERN, text)
    otps = re.findall(OTP_PATTERN, text)

    if urls:
        return {
            "success": True,
            "type": "link",
            "value": urls[-1],
            "message": text
        }

    if otps:
        return {
            "success": True,
            "type": "otp",
            "otp": otps[-1],
            "value": otps[-1],
            "message": text
        }

    if text.strip():
        return {
            "success": True,
            "type": "text",
            "value": text.strip(),
            "message": text.strip()
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

import os
import re
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
TG_STRING_SESSION = os.getenv("TG_STRING_SESSION", "")

OTP_PATTERN = r"\b\d{4,8}\b"
TIMEOUT_SECONDS = 60

app = FastAPI()
client = TelegramClient(StringSession(TG_STRING_SESSION), API_ID, API_HASH)
lock = asyncio.Lock()


class OtpRequest(BaseModel):
    email: str
    botUsername: str


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
            return {"success": False, "message": "ไม่มีอีเมล"}

        if not bot_username:
            return {"success": False, "message": "ไม่มี BotUsername"}

        if not await client.is_user_authorized():
            return {
                "success": False,
                "message": "Telegram ยังไม่ได้ล็อกอิน ต้องใส่ TG_STRING_SESSION ก่อน"
            }

        try:
            bot = await client.get_entity(bot_username)

            await client.send_message(bot, email)

            start_time = asyncio.get_event_loop().time()

            while True:
                if asyncio.get_event_loop().time() - start_time > TIMEOUT_SECONDS:
                    return {"success": False, "message": "บอท Maker ไม่ตอบกลับ"}

                messages = await client.get_messages(bot, limit=5)

                for msg in messages:
                    text = msg.message or ""

                    otp = find_otp(text)
                    if otp:
                        return {"success": True, "otp": otp}

                    if msg.buttons:
                        clicked = await click_otp_button(msg)
                        if clicked:
                            await asyncio.sleep(3)
                            break

                await asyncio.sleep(2)

        except Exception as e:
            return {"success": False, "message": str(e)}


def find_otp(text: str):
    match = re.search(OTP_PATTERN, text)
    return match.group(0) if match else None


async def click_otp_button(message):
    keywords = ["otp", "OTP", "code", "Code", "ขอ", "รหัส", "รับรหัส"]

    for row_index, row in enumerate(message.buttons):
        for col_index, button in enumerate(row):
            button_text = button.text or ""

            if any(k in button_text for k in keywords):
                await message.click(row_index, col_index)
                return True

    return False

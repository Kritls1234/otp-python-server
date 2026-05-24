FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

WORKDIR /app

COPY requirements.txt .

# ติดตั้ง dependencies (ไม่รวม playwright เพราะ base image มีแล้ว)
RUN pip install --no-cache-dir fastapi uvicorn[standard] pydantic telethon httpx gunicorn

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

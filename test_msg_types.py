import asyncio
from aiogram import Bot, Dispatcher, types, F
import os

bot = Bot(token=os.environ.get("TELEGRAM_BOT_TOKEN", "123:abc"))
dp = Dispatcher()

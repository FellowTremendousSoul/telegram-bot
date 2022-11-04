from aiogram import Bot
from aiogram.dispatcher import Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage

from setting.config import BOT_TOKEN

storage = MemoryStorage()

bot = Bot(BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot, storage=storage)
from aiogram import Bot
from aiogram.utils import executor

from create_bot import dp, bot
from handlers import user, admin
from data_base import sql_server_db
from services import command


async def on_startup(_):
    print('Бот запущен')
    sql_server_db.sql_server_start()
    await command.set_command(bot)

admin.register_message_handler_admin(dp)
user.register_handlers_client(dp)

# команда запуска бота
executor.start_polling(dp, skip_updates=True, on_startup=on_startup)

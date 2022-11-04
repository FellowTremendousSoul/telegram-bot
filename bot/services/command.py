from aiogram import Bot
from aiogram.types import BotCommand, BotCommandScopeDefault


async def set_command(bot: Bot):
    return await bot.set_my_commands(
        commands=[
            BotCommand('start', 'Запуск бота'),
            BotCommand('menu', 'Главное меню'),
            BotCommand('help', 'Помощь')

        ],
        scope=BotCommandScopeDefault())
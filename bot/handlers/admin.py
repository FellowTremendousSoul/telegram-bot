import re


from aiogram import types, Dispatcher
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

from keyboards import inline
from handlers import user


class FSMAdmin(StatesGroup):
    contact = State()


async def command_start_contact(message: types.Message):
    markup = await inline.contact_phone_keyboard()
    await FSMAdmin.contact.set()
    await message.answer("Здравствуйте! \n"
                         "Давайте подтвердим Вашу личность \n"
                         "Нажмите на кнопку и отправьте Ваш номер телефона для подтверждения", reply_markup=markup)


async def load_contact(message: types.Message, state:FSMContext):
    message_phone = message.contact.phone_number
    message_phone = message_phone.replace('+','')
    message_user_id = message.from_user.id

    with open("./phone.txt", "r") as f:
        for line in f:
            text_phone = re.sub("^\s+|\n|\r|\s+$", '', line)

            if message_phone == text_phone:
                with open('./user_id.txt', 'a') as f:
                    f.write(f'{message_user_id}:{text_phone} \n')
                await state.finish()
                await message.answer('Добро пожаловать!', reply_markup=inline.ReplyKeyboardRemove())
                await user.command_start(message)
                break
        else:
            await state.finish()
            await message.answer("У Вас нет доступа", reply_markup=inline.ReplyKeyboardRemove())


def register_message_handler_admin(dp: Dispatcher):
    dp.register_message_handler(command_start_contact, commands="start", state=None)
    dp.register_message_handler(load_contact, content_types=['contact'], state=FSMAdmin.contact)
import re

from typing import Union

from aiogram import types, Dispatcher
from aiogram.dispatcher.filters import Text

from config import ADMINS
from data_base import sql_server_db
from keyboards import inline
from keyboards.inline import fuel_cd


async def check_phone(from_user_id):
    check = 0
    with open('./user_id.txt', 'r') as f:
        for line in f:
            user_id_and_phone = re.sub("^\s+|\n|\r|\s+$", '', line)
            dick_user = user_id_and_phone.split(":")

            with open("./phone.txt", "r") as file:
                for file_line in file:
                    text_phone = re.sub("^\s+|\n|\r|\s+$", '', file_line)

                    if from_user_id == int(dick_user[0]) and text_phone == dick_user[1]:
                        check = 1
                        break
    return check


# стартовые команды /start, /menu, /help
async def command_start(message: Union[types.Message, types.CallbackQuery]):

    check = await check_phone(message.from_user.id)
    if check == 1:

        markup = await inline.list_gas_station_location()
        if isinstance(message, types.Message):
            await message.answer(f'Выберите в меню регион АЗС \n', reply_markup=markup)

        elif isinstance(message, types.CallbackQuery):
            callback = message
            await callback.message.edit_text(f'Главное меню \n')
            await callback.message.edit_reply_markup(reply_markup=markup)
            await callback.answer()
    elif check == 0:
        await message.answer('У Вас запрещен доступ')


async def command_menu(message: types.Message):
    check = await check_phone(message.from_user.id)
    if check == 1:
        markup = await inline.list_gas_station_location()
        await message.answer(f'Главное меню \n', reply_markup=markup)
    elif check == 0:
        await message.answer('У Вас запрещен доступ')


async def command_help(message: types.Message):
    await message.answer('/start - команда запуска бота и вызова главного меню \n'
                         'При вызове команды /start, происходит проверка доступа\n \n'
                         '/menu - команады вызова главного меню' )


# разбивка на регионы: Москва и москвоская облвсть
async def command_gas_stations_moscow(callback: types.CallbackQuery, callback_data: dict):
    check = await check_phone(callback.message.chat.id)
    if check == 1:
        action = callback_data["action"]
        markup = await inline.list_gas_station_moscow(action)
        await callback.message.edit_text(f'<i>Выберите АЗС</i> \nМосква')
        await callback.message.edit_reply_markup(reply_markup=markup)
        await callback.answer()
    elif check == 0:
        await callback.answer('У Вас запрещен доступ')



async def command_gas_stations_mo(callback: types.CallbackQuery, callback_data: dict):
    check = await check_phone(callback.message.chat.id)
    if check == 1:
        action = callback_data["action"]
        markup = await inline.list_gas_station_mo(action)
        await callback.message.edit_text(f'<i>Выберите АЗС</i> \nМосковская область')
        await callback.message.edit_reply_markup(reply_markup=markup)
        await callback.answer()
    elif check == 0:
        await callback.answer('У Вас запрещен доступ')


# списки всех резурвуаров со значениями > 50% и > 20%
async def command_values(callback: types.CallbackQuery, callback_data: dict):
    check = await check_phone(callback.message.chat.id)
    if check == 1:
        await callback.answer(text="Загружаем...")
        action = callback_data["gas_station_id"]
        action_back = callback_data["action_back"]
        average_values = await sql_server_db.read_values_moscow(action) if action_back == "gs_moscow" \
            else await sql_server_db.read_values_mo(action)
        # average_values = await sql_server_db.read_average_values(action_back)
        markup = await inline.button_back_keyboard(0, action_back, 0) \
            if not average_values[1] else await inline.pages_keyboard(action, action_back)
        await callback.message.edit_text('\n'.join(average_values[0]))
        await callback.message.edit_reply_markup(reply_markup=markup)
        await callback.answer()
    elif check == 0:
        await callback.answer('У Вас запрещен доступ')


# вторая станица для списка всех резервуаров
async def command_values_two_pages(callback: types.CallbackQuery, callback_data: dict):
    check = await check_phone(callback.message.chat.id)
    if check == 1:
        await callback.answer(text="Загружаем...")
        region = callback_data["gas_station_id"]
        action_back = callback_data["action_back"]
        action = callback_data['action']
        average_values = await sql_server_db.read_values_moscow(action_back) if action_back == "gs_moscow" \
            else await sql_server_db.read_values_mo(action_back)
        markup = await inline.button_back_keyboard(action_back, 'values', region)
        await callback.message.edit_text('\n'.join(average_values[1]))
        await callback.message.edit_reply_markup(reply_markup=markup)
        await callback.answer()
    elif check == 0:
        await callback.answer('У Вас запрещен доступ')


# стартовая команда по работе с азс
# разбивка строки типа: "АЗС 060" на число > запрос на поиск id по номеру из строки > запрос на азс по id
'''async def command_gas_station_mess(message: types.Message):
    gas_station_number_match = re.findall(r'\d+', message.text)
    gas_station_number_str = ''.join(gas_station_number_match)
    gas_statin_id = await sql_server_db.search_gas_station_id(gas_station_number_str)
    gas_station = await sql_server_db.read_gas_station(gas_statin_id)

    markup = await inline.list_fuel_tanks_keyboard(gas_station['GasStationId'], 'start_message')
    await message.answer_location(gas_station['GpsLatitude'], gas_station['GpsLongitude'])
    await message.answer(gas_station['message_gas_station'], reply_markup=markup)
'''


# информация о АЗС
async def command_gas_station_call(callback: types.CallbackQuery, callback_data: dict):
    check = await check_phone(callback.message.chat.id)
    if check == 1:
        gs_id = callback_data["gas_station_id"]
        action_back = callback_data["action_back"]
        gas_station = await sql_server_db.read_gas_station(gs_id)
        markup = await inline.list_fuel_tanks_keyboard(gas_station['GasStationId'], action_back)
        await callback.message.edit_text(gas_station['message_gas_station'])
        await callback.message.edit_reply_markup(reply_markup=markup)
        await callback.answer()
    elif check == 0:
        await callback.answer('У Вас запрещен доступ')


# список всех резервуаров АЗС
async def command_fuel_tanks_all(callback: types.CallbackQuery, callback_data: dict):
    check = await check_phone(callback.message.chat.id)
    if check == 1:
        gs_id = callback_data["gas_station_id"]
        action_back = callback_data["action_back"]
        result_proc = await sql_server_db.read_procedure_current_fuel(gs_id, 0)
        markup = await inline.button_back_keyboard(gs_id, "gasStationCall", action_back)
        await callback.message.edit_text('\n'.join(result_proc))
        await callback.message.edit_reply_markup(reply_markup=markup)
        await callback.answer()
    elif check == 0:
        await callback.answer('У Вас запрещен доступ')


# информация об одном резервуаре
async def command_fuel_tank(callback: types.CallbackQuery, callback_data: dict):
    check = await check_phone(callback.message.chat.id)
    if check == 1:
        gs_id = callback_data["gas_station_id"]
        fuel_tank_id = callback_data["fuel_tank_id"]
        action_back = callback_data["action_back"]
        result_proc = await sql_server_db.read_procedure_current_fuel(gs_id, fuel_tank_id)
        markup = await inline.button_back_keyboard(gs_id, "gasStationCall", action_back)
        await callback.message.edit_text('\n'.join(result_proc))
        await callback.message.edit_reply_markup(reply_markup=markup)
        await callback.answer()
    elif check == 0:
        await callback.answer('У Вас запрещен доступ')


async def command_id(message: types.Message):
    await message.reply(message.from_user.id)


def register_handlers_client(dp: Dispatcher):
    # dp.register_message_handler(command_start, commands=['start'])
    dp.register_callback_query_handler(command_start, text="start_message")
    dp.register_message_handler(command_menu,  commands=['menu'])
    dp.register_message_handler(command_help, commands=['help'])
    dp.register_callback_query_handler(command_gas_stations_moscow, fuel_cd.filter(action="gs_moscow"))
    dp.register_callback_query_handler(command_gas_stations_mo, fuel_cd.filter(action="gs_mo"))
    # dp.register_message_handler(command_gas_station_mess, Text(contains='азс', ignore_case=True))
    dp.register_callback_query_handler(command_values, fuel_cd.filter(action='values'))
    dp.register_callback_query_handler(command_values_two_pages, fuel_cd.filter(action="AverageTwoPages"))
    dp.register_callback_query_handler(command_gas_station_call, fuel_cd.filter(action="gasStationCall"))
    dp.register_callback_query_handler(command_fuel_tanks_all, fuel_cd.filter(action="FuelTanksAll"))
    dp.register_callback_query_handler(command_fuel_tank, fuel_cd.filter(action="FuelTank"))
    # dp.register_message_handler(command_id, commands=['id'])




from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.utils.callback_data import CallbackData

from data_base import sql_server_db


fuel_cd = CallbackData("Test", "action", "action_back", "gas_station_id", "fuel_tank_id")


async def list_gas_station_location():
    kb_button = InlineKeyboardMarkup(row_width=2)
    button_moscow = InlineKeyboardButton(text='Москва',
                                         callback_data=fuel_cd.new(action="gs_moscow",
                                                                   action_back=0,
                                                                   gas_station_id=0,
                                                                   fuel_tank_id=0))

    button_mo = InlineKeyboardButton(text='Московская область',
                                     callback_data=fuel_cd.new(action="gs_mo",
                                                               action_back=0,
                                                               gas_station_id=0,
                                                               fuel_tank_id=0))
    kb_button.row(button_moscow).row(button_mo)

    return kb_button


async def contact_phone_keyboard():
    markup_request = ReplyKeyboardMarkup(resize_keyboard=True).add(
        KeyboardButton('Отправить свой контакт ☎️', request_contact=True)
    )

    return markup_request


async def list_gas_station_moscow(action):
    inline_kb_gs_list = InlineKeyboardMarkup(row_width=3)
    list_gas_stations = await sql_server_db.read_list_gas_station()
    for row in list_gas_stations:
        gs_number = row.GasStationNumber
        if not gs_number.startswith('0'):
            inline_kb_gs_list.insert(
                InlineKeyboardButton(text=f'{row.GasStationName}',
                                     callback_data=fuel_cd.new(action="gasStationCall",
                                                               action_back=action,
                                                               gas_station_id=row.GasStationId,
                                                               fuel_tank_id=0)))
    inline_kb_gs_list.add(InlineKeyboardButton(text="🟡 Заполненность  < 50%",
                                               callback_data=fuel_cd.new(action='values',
                                                                         action_back=action,
                                                                         gas_station_id="average",
                                                                         fuel_tank_id=0)))
    inline_kb_gs_list.add(InlineKeyboardButton(text="🔴 Заполненность  < 20%",
                                               callback_data=fuel_cd.new(action='values',
                                                                         action_back=action,
                                                                         gas_station_id="critical",
                                                                         fuel_tank_id=0)))
    inline_kb_gs_list.row(InlineKeyboardButton(text='⬅ Назад', callback_data='start_message'))

    return inline_kb_gs_list


async def list_gas_station_mo(action):
    inline_kb_gs_list = InlineKeyboardMarkup(row_width=3)
    list_gas_stations = await sql_server_db.read_list_gas_station()
    for row in list_gas_stations:
        gs_number = row.GasStationNumber
        if gs_number.startswith('0'):
            inline_kb_gs_list.insert(
                InlineKeyboardButton(text=f'{row.GasStationName}',
                                     callback_data=fuel_cd.new(action="gasStationCall",
                                                               action_back=action,
                                                               gas_station_id=row.GasStationId,
                                                               fuel_tank_id=0)))
    inline_kb_gs_list.add(InlineKeyboardButton(text="🟡 Заполненность  < 50%",
                                               callback_data=fuel_cd.new(action='values',
                                                                         action_back=action,
                                                                         gas_station_id="average",
                                                                         fuel_tank_id=0)))
    inline_kb_gs_list.add(InlineKeyboardButton(text="🔴 Заполненность  < 20%",
                                               callback_data=fuel_cd.new(action='values',
                                                                         action_back=action,
                                                                         gas_station_id="critical",
                                                                         fuel_tank_id=0)))
    inline_kb_gs_list.row(InlineKeyboardButton(text='⬅ Назад', callback_data='start_message'))

    return inline_kb_gs_list


async def pages_keyboard(action, action_back): #average, #gs_mo
    inline_kb_back = InlineKeyboardMarkup(row_width=1)
    button_forward = InlineKeyboardButton(text="➡ Следующая страница ",
                                          callback_data=fuel_cd.new(action="AverageTwoPages", #average_two_pages
                                                                    action_back=action, #average
                                                                    gas_station_id=action_back, #gs_mo - нужно сохранить информацию для 3 уровня. запихала сюда
                                                                    fuel_tank_id=0))
    button_back = InlineKeyboardButton(text="⬅ Назад",
                                       callback_data=fuel_cd.new(action=action_back, #gs_mo
                                                                 action_back=0, #0
                                                                 gas_station_id=0,
                                                                 fuel_tank_id=0))
    inline_kb_back.row(button_forward).row(button_back)

    return inline_kb_back


async def list_fuel_tanks_keyboard(gs_id, action_back):
    inline_kb_tanks = InlineKeyboardMarkup(row_width=2)
    fuel_tanks_col = await sql_server_db.read_fuel_tanks_list(gs_id)
    for row in fuel_tanks_col:
        bt_tank_num = row.FuelTankNumber
        bt_tank_name = row.FuelTypeName
        tank_id = row.FuelTankId
        inline_kb_tanks.insert(
            InlineKeyboardButton(text=f'№ {bt_tank_num}  {bt_tank_name}',
                                 callback_data=fuel_cd.new(action="FuelTank",
                                                           action_back=action_back,
                                                           gas_station_id=gs_id,
                                                           fuel_tank_id=tank_id)))
    inline_kb_tanks.row(InlineKeyboardButton(text="🛢 Все резервуары",
                                             callback_data=fuel_cd.new(action="FuelTanksAll",
                                                                       action_back=action_back,
                                                                       gas_station_id=gs_id,
                                                                       fuel_tank_id=0)))
    inline_kb_tanks.row(InlineKeyboardButton(text='⬅ Назад',
                                             callback_data=fuel_cd.new(action=action_back,
                                                                       action_back=0,
                                                                       gas_station_id=gs_id,
                                                                       fuel_tank_id=0)))

    return inline_kb_tanks


async def button_back_keyboard(gs_id, action, action_back):
    inline_kb_back = InlineKeyboardMarkup(row_width=1)
    button_back = InlineKeyboardButton(text="⬅ Назад",
                                       callback_data=fuel_cd.new(action=action,
                                                                 action_back=action_back,
                                                                 gas_station_id=gs_id,
                                                                 fuel_tank_id=0))
    inline_kb_back.row(button_back)

    return inline_kb_back




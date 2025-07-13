from aiogram.types import InputFile, ReplyKeyboardMarkup, KeyboardButton
import pytz
from numpy import nan
from datetime import time as t, datetime, timedelta
import asyncio
import logging
import re
import json
import os
import gspread
import requests
import pandas as pd
import tempfile
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.dispatcher import FSMContext
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram.utils.exceptions import MessageNotModified
from aiogram.dispatcher.handler import CancelHandler
from aiogram.dispatcher.middlewares import BaseMiddleware
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import time


from config import API_TOKEN, CONFIG_URL, ADMIN_IDS, CREDS, CONFIG_SHEET_ID
from Wb_bot import get_available_users_from_config, get_user_cabinets, generate_report, main_from_config
from WB_orders import get_wb_product_cards

# Добавляем клавиатуру с кнопкой "Главное меню"
main_menu_keyboard = ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("Главное меню"))

# Инициализация бота и планировщика
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
scheduler = AsyncIOScheduler()

# Настройки времени
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Время отчетов по умолчанию (6:00 МСК)
DEFAULT_TIME = t(6, 0, tzinfo=MOSCOW_TZ)

# Файл для сохранения данных
DATA_FILE = "user_data.json"

# Добавляем константы для оплаты
SUBSCRIPTION_PRICE = 500
YOOKASSA_PAYMENT_URL = "https://yookassa.ru/"

# Состояния для добавления пользователя
class UserRegistrationStates(StatesGroup):
    WAITING_API_KEY = State()
    WAITING_CABINET_NAME = State()

# Состояния для добавления кабинета
class AddCabinetStates(StatesGroup):
    WAITING_API_KEY = State()
    WAITING_CABINET_NAME = State()

# Состояния для управления кабинетами
class ManageCabinetStates(StatesGroup):
    SELECT_CABINET = State()
    ACTION_CHOICE = State()
    WAITING_NEW_NAME = State()

# Состояния для поддержки
class SupportStates(StatesGroup):
    WAITING_QUESTION = State()
    WAITING_REPLY = State()

class BroadcastStates(StatesGroup):
    WAITING_MESSAGE = State()
    CONFIRMATION = State()


# Регулярные выражения для валидации
EMAIL_REGEX = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

# Инициализация Google Sheets API
gc = gspread.authorize(CREDS)

# Глобальный словарь для отслеживания активных запросов

class ActiveRequestMiddleware(BaseMiddleware):
    async def on_pre_process_callback_query(self, callback_query: types.CallbackQuery, data: dict):
        user_id = callback_query.from_user.id
        # Разрешаем кнопку "Главное меню"
        if callback_query.data == "back_to_main":
            return
        
# Собственная реализация rate limiter
class RateLimiterMiddleware(BaseMiddleware):
    def __init__(self, limit=3, interval=5):
        self.limit = limit
        self.interval = interval
        self.users = defaultdict(list)
        super().__init__()

    async def on_pre_process_message(self, message: types.Message, data: dict):
        await self.check_rate_limit(message)

    async def on_pre_process_callback_query(self, callback_query: types.CallbackQuery, data: dict):
        await self.check_rate_limit(callback_query)

    async def check_rate_limit(self, event):
        user_id = event.from_user.id
        current_time = time.time()
        
        # Очищаем устаревшие временные метки
        self.users[user_id] = [t for t in self.users[user_id] if current_time - t < self.interval]
        
        if len(self.users[user_id]) >= self.limit:
            await event.answer("⏳ Слишком много запросов. Пожалуйста, подождите.")
            raise CancelHandler()
        
        self.users[user_id].append(current_time)

# Регистрируем middleware для rate limiting
dp.middleware.setup(RateLimiterMiddleware(limit=4, interval=5))
dp.middleware.setup(ActiveRequestMiddleware())

def get_cancel_keyboard():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))
    return kb

def get_cancel_admin_keyboard():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_action_admin"))
    return kb

def validate_cabinet_name(name: str) -> bool:
    return 2 <= len(name.strip()) <= 50

def validate_wb_api_key(api_key: str) -> bool:
    url_stat = "https://seller-analytics-api.wildberries.ru/ping"
    url_ads = "https://advert-api.wildberries.ru/ping"
    headers = {"Authorization": api_key}

    try:
        response_stat = requests.get(url_stat, headers=headers)
    except Exception as e:
        logging.error(f"Ошибка проверки API ключа: {e}")
        return False
    try:
        response_ads = requests.get(url_ads, headers=headers)
    except Exception as e:
        logging.error(f"Ошибка проверки API ключа: {e}")
        return False
    
    return response_stat.status_code == 200 and response_ads.status_code == 200

class UserDataCache:
    def __init__(self):
        self.user_lk_cache = {}
        self.config_cache = None
        self.last_config_update = None
        self.cache_expiration = timedelta(days=1)
        self.user_mapping = {}
        self.user_spreadsheet_urls = {}

    async def load_data(self):
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, 'r') as f:
                    data = json.load(f)
                    self.user_mapping = {int(k): v for k, v in data.get('user_mapping', {}).items()}
                    self.user_spreadsheet_urls = data.get('user_spreadsheet_urls', {})
            except Exception as e:
                logging.error(f"Ошибка загрузки данных: {e}")

    async def save_data(self):
        data = {
            'user_mapping': self.user_mapping,
            'user_spreadsheet_urls': self.user_spreadsheet_urls
        }
        try:
            with open(DATA_FILE, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logging.error(f"Ошибка сохранения данных: {e}")

    async def update_config_cache(self):
        try:
            
            users = get_available_users_from_config(CONFIG_URL)
            config = {}
            for user in users:
                cabinets = get_user_cabinets(CONFIG_URL, user)
                config[user] = cabinets
            self.config_cache = config
            self.last_config_update = datetime.now()
            return config
        except Exception as e:
            logging.error(f"Ошибка обновления кэша конфигурации: {e}")
            return None

    async def get_config_cache(self):
        if self.config_cache is None or (self.last_config_update and
                                         (datetime.now() - self.last_config_update) > self.cache_expiration):
            await self.update_config_cache()
        return self.config_cache

    async def get_user_cabinets(self, username: str):
        config = await self.get_config_cache()
        return config.get(username, []) if config else []

    async def get_available_users(self):
        config = await self.get_config_cache()
        return list(config.keys()) if config else []

    async def get_available_users_for_admin(self):
        return await self.get_available_users()

    async def get_available_users_for_user(self, telegram_id: int):
        return [self.user_mapping.get(telegram_id)]

    async def bind_user(self, telegram_id: int, username: str):
        self.user_mapping[telegram_id] = username
        await self.save_data()

# Инициализация кэша
cache = UserDataCache()

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def show_main_menu(chat_id, message_text="Выберите действие:"):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("📊 Получить отчет", callback_data="get_report"),
        InlineKeyboardButton("🛠 Управление кабинетами", callback_data="manage_cabinets"),
        InlineKeyboardButton("📋 Моя таблица", callback_data="show_spreadsheet")
    )
    kb.row(
        InlineKeyboardButton("❓ Ответы на вопросы", callback_data="faq"),
        InlineKeyboardButton("Поддержка", callback_data="support")
    )
    await bot.send_message(chat_id, message_text, reply_markup=kb)

async def show_admin_menu(chat_id, message_text="Выберите действие:"):
    admin_kb = InlineKeyboardMarkup()
    admin_kb.add(InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast"))
    await bot.send_message(chat_id, message_text, reply_markup=admin_kb)

# Функция для запуска блокирующих операций в отдельном потоке
def run_in_thread(func, *args):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        return loop.run_in_executor(pool, func, *args)

@dp.callback_query_handler(lambda c: c.data == "subscribe")
async def subscribe_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 Перейти к оплате", url=YOOKASSA_PAYMENT_URL))
    kb.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_main"))
    message = (
        "🤖 *Что умеет этот бот?*\n\n"
        "Автоматизированная аналитика для продавцов Wildberries:\n"
        "✅ Ежедневные отчеты в заданное время\n"
        "✅ Анализ прибыли по каждому артикулу\n"
        "✅ Автоматическое обновление данных\n"
        "✅ Поддержку до 7 личных кабинетов\n"
        f"Стоимость подписки: *{SUBSCRIPTION_PRICE} руб./месяц*\n\n"
        "После оплаты доступ будет активирован в течение 5 минут"
    )
    await bot.send_message(
        user_id,
        message,
        parse_mode="Markdown",
        reply_markup=kb
    )
    await callback.answer()

@dp.message_handler(commands=["start"])
async def start_handler(message: types.Message):
    user_id = message.from_user.id
    await cache.load_data()

    if is_admin(user_id):
        # Создаем клавиатуру для администратора
        admin_kb = InlineKeyboardMarkup()
        admin_kb.add(InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast"))
        
        await message.answer(
            "👋 Привет, администратор!\nВы будете получать уведомления об ошибках.",
            reply_markup=admin_kb
        )
        return

    if cache.user_mapping.get(user_id):
        await message.answer("👋 Вы уже зарегистрированы!\nВы можете добавить до 7 личных кабинетов")
        await show_main_menu(message.chat.id)
        return

    instruction_photo = InputFile("instruction.jpg")
    await bot.send_photo(message.chat.id, instruction_photo)
    await message.answer(
        "👋 Добро пожаловать! Для регистрации введите ваш WB API ключ (аналитика и продвижение):",
        reply_markup=get_cancel_keyboard()
    )
    await UserRegistrationStates.WAITING_API_KEY.set()

@dp.callback_query_handler(lambda c: c.data == "show_spreadsheet")
async def show_spreadsheet_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if is_admin(user_id):
        return
    
    username = cache.user_mapping.get(user_id)
    if not username:
        await callback.answer("❌ Вы не привязаны к аккаунту", show_alert=True)
        return
    
    spreadsheet_url = cache.user_spreadsheet_urls.get(username)
    if spreadsheet_url:
        message = (
            "📊 Ваша таблица с данными:\n"
            f"{spreadsheet_url}\n\n"
            "В этой таблице вы можете:\n"
            "1. Видеть все ваши артикулы\n"
            "2. Заполнять столбцы 'Прибыль' и 'Выкупаемость'\n"
            "3. После заполнения запрашивать отчеты"
        )
        await bot.send_message(user_id, message)
    else:
        await callback.answer("❌ Ссылка на таблицу не найдена", show_alert=True)
    await show_main_menu(callback.message.chat.id)

@dp.callback_query_handler(lambda c: c.data == "cancel_action", state="*")
async def cancel_action_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await show_main_menu(callback.message.chat.id)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "cancel_action_admin", state="*")
async def cancel_action_admin_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await show_admin_menu(callback.message.chat.id)
    await callback.message.delete()

@dp.message_handler(commands=["add_cabinet"])
async def add_cabinet_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if is_admin(user_id):
        return
    
    username = cache.user_mapping.get(user_id)
    if not username:
        await message.answer("❌ Сначала привяжите аккаунт с помощью /start")
        return

    cabinets = await cache.get_user_cabinets(username)
    if len(cabinets) > 6:
        await message.answer("❌ Достигнут лимит в 7 кабинетов")
        return

    instruction_photo = InputFile("instruction.jpg")
    await bot.send_photo(message.chat.id, instruction_photo)
    await message.answer("Введите WB API ключ (аналитика и продвижение) для нового кабинета:", reply_markup=get_cancel_keyboard())
    async with state.proxy() as data:
        data['username'] = username
    await AddCabinetStates.WAITING_API_KEY.set()

@dp.message_handler(state=AddCabinetStates.WAITING_API_KEY)
async def process_cabinet_api_key(message: types.Message, state: FSMContext):
    api_key = message.text.strip()
    if not api_key:
        await message.answer("❌ API ключ не может быть пустым!")
        return

    msg = await message.answer("⏳ Ожидайте 30 секунд, идёт обработка...")
    if not validate_wb_api_key(api_key):
        await message.answer("❌ Неверный API ключ! Проверьте ключ и попробуйте снова.")
        return

    async with state.proxy() as data:
        data['api_key'] = api_key

    await AddCabinetStates.next()
    await msg.edit_text("✅ Ключ принят! Теперь введите название для нового кабинета:", reply_markup=get_cancel_keyboard())

@dp.message_handler(state=AddCabinetStates.WAITING_CABINET_NAME)
async def process_new_cabinet_name(message: types.Message, state: FSMContext):
    cabinet_name = message.text.strip()
    if not validate_cabinet_name(cabinet_name):
        await message.answer("❌ Название кабинета должно быть от 2 до 50 символов!")
        return

    async with state.proxy() as data:
        username = data['username']
        api_key = data['api_key']

    wait_message = await message.answer("🔄 Ожидайте 30 сек, идёт добавление кабинета и обновление артикулов...", reply_markup=main_menu_keyboard)
    try:
        success = await run_in_thread(add_cabinet_to_user, username, api_key, cabinet_name)
        if success:
            response = f"✅ Кабинет '{cabinet_name}' успешно добавлен! Артикулы добавлены в вашу таблицу."
        else:
            response = "❌ Не удалось добавить кабинет. Обратитесь к администратору."
        
        await message.answer(response)
        await show_main_menu(message.chat.id)
    except Exception as e:
        logging.error(f"Ошибка при добавлении кабинета: {e}")
        await message.answer("❌ Произошла ошибка при добавлении кабинета")
    finally:
        await state.finish()
        try:
            await bot.delete_message(message.chat.id, wait_message.message_id)
        except:
            pass

@dp.message_handler(state=UserRegistrationStates.WAITING_API_KEY)
async def process_registration_api_key(message: types.Message, state: FSMContext):
    api_key = message.text.strip()
    if not api_key:
        await message.answer("❌ API ключ не может быть пустым!")
        return

    msg = await message.answer("⏳ Ожидайте 30 секунд, идёт обработка...")
    if not validate_wb_api_key(api_key):
        await message.answer("❌ Неверный API ключ! Проверьте ключ и попробуйте снова.")
        return

    async with state.proxy() as data:
        data['api_key'] = api_key

    await UserRegistrationStates.next()
    await msg.edit_text("✅ Ключ принят! Теперь введите название для вашего личного кабинета:", reply_markup=get_cancel_keyboard())


@dp.message_handler(state=UserRegistrationStates.WAITING_CABINET_NAME)
async def process_registration_cabinet_name(message: types.Message, state: FSMContext):
    cabinet_name = message.text.strip()
    if not validate_cabinet_name(cabinet_name):
        await message.answer("❌ Название кабинета должно быть от 2 до 50 символов!")
        return

    async with state.proxy() as data:
        api_key = data['api_key']

    # Создаем уникальное имя пользователя
    username = f"user_{message.from_user.id}"

    # Получаем свободную таблицу из пула
    spreadsheet_info = await run_in_thread(get_available_spreadsheet, username)
    if not spreadsheet_info:
        await message.answer("❌ Нет доступных таблиц. Обратитесь к администратору.")
        await state.finish()
        return

    # Предоставляем доступ
    await run_in_thread(grant_spreadsheet_access, spreadsheet_info['id'])

    # Добавляем пользователя в конфигурацию
    await run_in_thread(
        add_user_to_config,
        username,
        api_key,
        cabinet_name,
        spreadsheet_info['url']
    )

    # Сохраняем данные
    cache.user_spreadsheet_urls[username] = spreadsheet_info['url']
    await cache.bind_user(message.from_user.id, username)
    await cache.save_data()

    # Инициализируем таблицу
    spreadsheet = gc.open_by_url(spreadsheet_info['url'])
    success = await run_in_thread(add_cabinet_sheet, spreadsheet, cabinet_name, api_key)
    
    if success:
        await message.answer(
            "✅ Регистрация успешно завершена!\n"
            f"• Ваш аккаунт: {username}\n"
            f"• Ваш кабинет: {cabinet_name}\n"
            f"• Ваша таблица: {spreadsheet_info['url']}\n\n"
            "Теперь вы можете добавлять до 7 личных кабинетов"
        )
    else:
        await message.answer("❌ Ошибка при инициализации таблицы")

    await state.finish()
    await show_main_menu(message.chat.id)


@dp.callback_query_handler(lambda c: c.data == "get_report")
async def get_report_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    if not is_admin(user_id):
        users = await cache.get_available_users_for_user(user_id)
        if not users or not users[0]:
            try:
                await callback.message.edit_text("⚠️ Вы не привязаны ни к одному пользователю.")
            except MessageNotModified:
                await callback.answer()
            await show_main_menu(callback.message.chat.id)
            return

        username = users[0]
        cabinets = await cache.get_user_cabinets(username)

        if not cabinets:
            try:
                await callback.message.edit_text(f"⚠️ У пользователя {username} нет доступных личных кабинетов.")
            except MessageNotModified:
                await callback.answer()
            await show_main_menu(callback.message.chat.id)
            return

        keyboard = InlineKeyboardMarkup(row_width=1)

        for cabinet in cabinets:
            keyboard.add(InlineKeyboardButton(
                text=cabinet, callback_data=f"get_report:{username}:{cabinet}"))
        
        keyboard.add(InlineKeyboardButton(
            text="Все", callback_data=f"get_report:{username}:all"))

        keyboard.add(InlineKeyboardButton(
            "🔙 Назад", callback_data="back_to_main"))
        
        try:
            await callback.message.edit_text(f"Выберите личный кабинет:", reply_markup=keyboard)
        except MessageNotModified:
            await callback.answer()
            
            
@dp.callback_query_handler(lambda c: c.data == "back_to_main")
async def back_to_main_callback(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await show_main_menu(callback.message.chat.id)
    await callback.message.delete()

async def send_report_as_file(chat_id: int, username: str, cabinet_name: str, df: pd.DataFrame, summary: str):
    try:
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
            file_path = temp_file.name

        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            df = df.fillna(value=" ВНЕСИТЕ")
            df.to_excel(writer, sheet_name='Отчет', index=False)
            workbook = writer.book
            worksheet = writer.sheets['Отчет']

            for idx, col in enumerate(df.columns):
                max_len = max(
                    df[col].astype(str).map(len).max(),
                    len(str(col))
                )
                worksheet.set_column(idx, idx, max_len + 2)

            start_row = len(df) + 3
            header_format = workbook.add_format({
                'bold': True,
                'font_size': 10,
                'bottom': 1
            })
            value_format = workbook.add_format({
                'font_size': 12,
                'align': 'right',
                'num_format': '#,##0.00'
            })

            worksheet.write(start_row, 0, "СВОДКА ПО ОТЧЕТУ", header_format)
            parts = summary.split(':')
            worksheet.write(start_row, 1, parts[0].strip(), value_format)
            worksheet.write(start_row, 2, parts[1].strip(), value_format)
            worksheet.write(start_row, 3, parts[2].strip(), value_format)

        timestamp = datetime.now().strftime("%Y%m%d %H_%M")
        file_name = f"Отчет_{cabinet_name}_{timestamp}.xlsx"
        excel_file = InputFile(file_path, filename=file_name)

        await bot.send_document(
            chat_id=chat_id,
            document=excel_file,
            caption=f"📊 Отчет по ЛК: {cabinet_name}"
        )
        os.unlink(file_path)
    except Exception as e:
        logging.error(f"Ошибка создания Excel-отчета: {e}")
        await bot.send_message(chat_id, "❌ Ошибка формирования отчета в формате Excel")

@dp.callback_query_handler(lambda c: c.data.startswith("get_report:"))
async def process_report_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if is_admin(user_id):
        return
    
    parts = callback.data.split(":")
    username = parts[1]
    cabinet = parts[2]
    
    try:
        wait_message = await callback.message.edit_text(
            text="🔄 Формирую отчёт, это займёт некоторое время...",
            reply_markup=None  # Убираем клавиатуру
        )
    except Exception as e:
        logging.error(f"Ошибка редактирования сообщения: {e}")
        # Если не удалось отредактировать, отправляем новое сообщение
        wait_message = await bot.send_message(user_id, "🔄 Формирую отчёт, это займёт некоторое время...")

    # wait_message = await bot.send_message(user_id, "🔄 Формирую отчёт, это займёт некоторое время...", reply_markup=main_menu_keyboard)
    try:
        if cabinet == "all":
            cabinets = await cache.get_user_cabinets(username)
            if not cabinets:
                await bot.send_message(user_id, f"⚠️ У пользователя {username} нет доступных личных кабинетов.")
                return

            summ = {'costs': 0.0, 'profit': 0.0}
            for cabinet_name in cabinets:
                # Запускаем в отдельном потоке с обработкой возобновления
                df, summary = await run_in_thread(
                    generate_report_with_resume, 
                    username, 
                    cabinet_name, 
                    CONFIG_URL
                )
                
                if summary == "429_error":
                    await bot.send_message(user_id, "⚠️ Превышен лимит запросов. Попробуйте позже")
                    return
                if df is not None and not df.empty:
                    summ_parts = summary.split(':')
                    summ["costs"] += float(summ_parts[1])
                    if pd.notna(summ_parts[2]):
                        summ["profit"] += float(summ_parts[2])

            await bot.send_message(user_id, 
                f"<b>Суммарный отчёт по всем кабинетам:</b>\n"
                f"• Сумма затрат: {round(summ['costs'], 2)} руб\n"
                f"• Сумма прибыли: {round(summ['profit'], 2)} руб",
                parse_mode="HTML"
            )
        else:
            df, summary = await run_in_thread(
                generate_report_with_resume, 
                username, 
                cabinet, 
                CONFIG_URL
            )
            
            if summary == "429_error":
                await bot.send_message(user_id, "⚠️ Превышен лимит запросов. Попробуйте позже")
                return
                
            if df is None or df.empty:
                await bot.send_message(user_id, f"ℹ️ Нет данных по {cabinet}")
            else:
                await send_report_as_file(user_id, username, cabinet, df, summary)
    finally:
        try:
            await bot.delete_message(user_id, wait_message.message_id)
        except:
            pass

    await show_main_menu(callback.message.chat.id)

# Новая функция для возобновления обработки
def generate_report_with_resume(username, cabinet, config_url):
    """Генерирует отчет с возможностью возобновления после 429 ошибки"""
    state = None
    max_attempts = 3
    
    for attempt in range(max_attempts):
        result = generate_report(username, cabinet, config_url)
        
        # Если получили состояние для возобновления
        if result[1] == "resume_state":
            state = result[2]
            wait_time = result[3]
            time.sleep(wait_time)
            continue
            
        return result
        
    return None, "429_error"


def add_articles_to_sheet(worksheet, articles):
    """Добавляет артикулы и баркоды в лист таблицы с сортировкой"""
    if not articles:
        return

    # Формируем данные для вставки
    values = []
    for article in articles:
        values.append(article)

    # Разбиваем на батчи по 500 строк
    batch_size = 500
    for i in range(0, len(values), batch_size):
        batch = values[i:i+batch_size]
        try:
            worksheet.append_rows(batch)
            time.sleep(1)
        except Exception as e:
            logging.error(f"Ошибка добавления артикулов: {e}")

    # Сортируем данные после вставки
    sort_sheet(worksheet)

def sort_sheet(worksheet):
    """Сортирует данные в листе по кабинету и артикулу продавца"""
    try:
        # Получаем все данные
        all_values = worksheet.get_all_values()
        if len(all_values) <= 3:  # инструкция + пустая строка + заголовки
            return

        # Сохраняем первые три строки (инструкция, пустая, заголовки)
        instruction_row = all_values[0]
        header_row = all_values[2]
        data = all_values[3:]  # Данные начинаются с 4-й строки

        # Сортируем по столбцу A (кабинет) и столбцу B (артикул продавца)
        sorted_data = sorted(data, key=lambda x: (x[0], x[1], x[2]))
        # Обновляем весь лист
        worksheet.clear()

        # Восстанавливаем структуру
        worksheet.append_row(instruction_row)
        worksheet.append_row(header_row, table_range='A3:E3')
        
        if sorted_data:
            worksheet.append_rows(sorted_data)

        # Восстанавливаем форматирование
        worksheet.format("A1", {
            "textFormat": {
                "bold": True,
                "fontSize": 14
            },
            "horizontalAlignment": "LEFT",
            "wrapStrategy": "WRAP"
        })
        worksheet.merge_cells("A1:E1")
        
        # Серый цвет для заголовков (строка 3)
        worksheet.format("A3:E3", {
            "backgroundColor": {
                "red": 0.9,
                "green": 0.9,
                "blue": 0.9
            },
            "textFormat": {
                "bold": True
            }
        })

    except Exception as e:
        logging.error(f"Ошибка сортировки листа: {e}")

def get_wb_articles(api_key: str):
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/"
    date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    params = {"dateFrom": date_from}
    headers = {"Authorization": api_key}

    try:
        cards = get_wb_product_cards(headers)
        nm_ids = [(product['nmID'], product['vendorCode']) for product in cards]
        unique_pairs = set()
        for item in nm_ids:
            nmId = str(item[0])
            supplierArticle = str(item[1])
            if nmId:
                unique_pairs.add((nmId, supplierArticle))

        return list(unique_pairs)
    except Exception as e:
        logging.error(f"Ошибка получения данных с WB API: {e}")
        return []

def extract_spreadsheet_id(url: str) -> str:
    """Извлекает ID таблицы из URL"""
    parts = url.split('/')
    for i, part in enumerate(parts):
        if part == 'd' and i + 1 < len(parts):
            return parts[i + 1]
    return url


def get_available_spreadsheet(username: str) -> dict:
    """Возвращает свободную таблицу из пула и помечает ее как занятую"""
    try:
        # Открываем таблицу с пулом таблиц
        pool_sheet = gc.open_by_key("1MZeib3KF9TDtH7S6ylfd4VzOLVA-tdcJfjF_HCJDHqY").sheet1
        records = pool_sheet.get_all_values()
        # print(records)
        # Ищем первую свободную таблицу (где столбец C пустой)
        for i, row in enumerate(records):
            if i == 0: continue  # Пропускаем заголовки
            if len(row) >= 3 and not row[2].strip():  # Столбец C пуст
                url = row[1].strip()
                # Помечаем таблицу как занятую
                pool_sheet.update_cell(i + 1, 3, username)
                return {'url': url, 'id': extract_spreadsheet_id(url)}
        return None
    except Exception as e:
        logging.error(f"Ошибка получения таблицы из пула: {e}")
        return None

def grant_spreadsheet_access(spreadsheet_id: str, email=""):
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        spreadsheet.share(None, perm_type='anyone', role='writer')
    except Exception as e:
        logging.error(f"Ошибка предоставления доступа: {e}")

def add_user_to_config(username: str, api_key: str, cabinet_name: str, spreadsheet_url: str):
    try:
        worksheet = gc.open_by_key(CONFIG_SHEET_ID).sheet1
        worksheet.append_row([username, api_key, cabinet_name, spreadsheet_url])
        cache.config_cache = None  # Сбрасываем кеш конфигурации
    except Exception as e:
        logging.error(f"Ошибка добавления пользователя в конфиг: {e}")

def add_cabinet_sheet(spreadsheet, cabinet_name: str, api_key: str):
    try:
        # Пытаемся получить лист "Маржа"
        try:
            worksheet = spreadsheet.worksheet("Маржа")
        except gspread.exceptions.WorksheetNotFound:
            # worksheet = spreadsheet.add_worksheet(title="Маржа", rows=1000, cols=5)
            worksheet = spreadsheet.get_worksheet(0)
            worksheet.update_title("Маржа")
            
            instruction = "Заполните столбцы 'Прибыль с ед. товара' и 'Выкупаемость (%)'. После заполнения можете запросить отчёт."
            worksheet.update(range_name='A1', values=[[instruction]])
            
            # Заголовки с серым фоном
            headers = ["Личный кабинет", "Артикул WB", "Артикул продавца",
                    "Прибыль с ед. товара", "Выкупаемость (%)"]
            worksheet.append_row(headers, table_range='A3:E3')
            
            # Форматирование
            worksheet.format("A1", {
                "textFormat": {
                    "bold": True,
                    "fontSize": 14
                },
                "horizontalAlignment": "LEFT",
                "wrapStrategy": "WRAP"
            })
            worksheet.merge_cells("A1:E1")
            
            # Серый цвет для заголовков (строка 3)
            worksheet.format("A3:E3", {
                "backgroundColor": {
                    "red": 0.9,
                    "green": 0.9,
                    "blue": 0.9
                },
                "textFormat": {
                    "bold": True
                }
            })

            # worksheet.freeze(rows=1)
        
        # Добавляем артикулы
        articles = get_wb_articles(api_key)
        articles_with_cabinet = [
            [cabinet_name, str(nmId), str(supplierArticle), "", ""]
            for (nmId, supplierArticle) in articles
        ]
        
        # Добавляем данные пачками
        batch_size = 100
        for i in range(0, len(articles_with_cabinet), batch_size):
            batch = articles_with_cabinet[i:i + batch_size]
            worksheet.append_rows(batch)
            time.sleep(1)  # Защита от лимитов API
        
        return True
    except Exception as e:
        logging.error(f"Ошибка инициализации таблицы: {e}")
        return False

def add_cabinet_to_user(username: str, api_key: str, cabinet_name: str):
    try:
        spreadsheet_url = cache.user_spreadsheet_urls.get(username)
        if not spreadsheet_url:
            logging.error(f"Для пользователя {username} не найдена таблица")
            return False

        # Добавляем в конфигурацию
        worksheet = gc.open_by_key(CONFIG_SHEET_ID).sheet1
        worksheet.append_row([username, api_key, cabinet_name, spreadsheet_url])
        cache.config_cache = None
        
        # Добавляем данные в таблицу пользователя
        spreadsheet = gc.open_by_url(spreadsheet_url)
        return add_cabinet_sheet(spreadsheet, cabinet_name, api_key)
    except Exception as e:
        logging.error(f"Ошибка добавления кабинета: {e}")
        return False


def get_cabinet_api_key(username: str, cabinet_name: str) -> str:
    try:
        worksheet = gc.open_by_key(CONFIG_SHEET_ID).sheet1
        records = worksheet.get_all_values()
        for row in records:
            if row[0] == username and row[2] == cabinet_name:
                return row[1]
        return None
    except Exception as e:
        logging.error(f"Ошибка получения API ключа: {e}")
        return None

@dp.callback_query_handler(lambda c: c.data == "manage_cabinets")
async def manage_cabinets_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    username = cache.user_mapping.get(user_id)

    if not username:
        await callback.answer("❌ Вы не привязаны к аккаунту")
        return

    cabinets = await cache.get_user_cabinets(username)
    cabinet_count = len(cabinets) if cabinets else 0

    kb = InlineKeyboardMarkup(row_width=1)
    if cabinet_count < 7:
        kb.add(InlineKeyboardButton(
            text="➕ Добавить кабинет",
            callback_data="add_cabinet_in_manage"
        ))

    if cabinet_count > 0:
        for cabinet in cabinets:
            kb.add(InlineKeyboardButton(
                text=cabinet,
                callback_data=f"select_cabinet:{cabinet}"
            ))
    else:
        try:
            await callback.message.edit_text(
                "У вас пока нет кабинетов. Хотите добавить первый?",
                reply_markup=kb
            )
            await ManageCabinetStates.SELECT_CABINET.set()
            return
        except MessageNotModified:
            await callback.answer()

    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_manage"))
    try:
        await callback.message.edit_text("Выберите кабинет для управления:", reply_markup=kb)
        await ManageCabinetStates.SELECT_CABINET.set()
    except MessageNotModified:
        await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("select_cabinet:"), state=ManageCabinetStates.SELECT_CABINET)
async def select_cabinet_callback(callback: types.CallbackQuery, state: FSMContext):
    cabinet_name = callback.data.split(":")[1]
    user_id = callback.from_user.id
    username = cache.user_mapping.get(user_id)

    async with state.proxy() as data:
        data['cabinet'] = cabinet_name
        data['username'] = username

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✏️ Переименовать", callback_data="rename_cabinet"),
        InlineKeyboardButton("❌ Удалить", callback_data="delete_cabinet"),
        InlineKeyboardButton("🔄 Обновить артикулы", callback_data="refresh_articles"),
        InlineKeyboardButton("🔙 Назад", callback_data="back_to_cabinets")
    )
    try:
        await callback.message.edit_text(
            f"Выбран кабинет: {cabinet_name}\nВыберите действие:",
            reply_markup=kb
        )
        await ManageCabinetStates.ACTION_CHOICE.set()
    except MessageNotModified:
        await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "add_cabinet_in_manage", state=ManageCabinetStates.SELECT_CABINET)
async def add_cabinet_in_manage_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = cache.user_mapping.get(user_id)

    if not username:
        await callback.answer("❌ Вы не привязаны к аккаунту")
        return

    cabinets = await cache.get_user_cabinets(username)
    cabinet_count = len(cabinets) if cabinets else 0

    if cabinet_count >= 7:
        await callback.answer("❌ Достигнут лимит в 7 кабинетов", show_alert=True)
        return
    
    try:
        await callback.message.delete()
        # bot.delete_message(
        #     chat_id=callback.message.chat.id,
        #     message_id=callback.message.message_id)
    except:
        pass
    instruction_photo = InputFile("instruction.jpg")    
    await bot.send_photo(callback.message.chat.id, instruction_photo)
    await callback.message.answer("Введите WB API ключ (аналитика и продвижение) для нового кабинета:", reply_markup=get_cancel_keyboard())
    async with state.proxy() as data:
        data['username'] = username
    await AddCabinetStates.WAITING_API_KEY.set()

@dp.callback_query_handler(lambda c: c.data == "back_to_cabinets", state="*")
async def back_to_cabinets_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await manage_cabinets_callback(callback)

@dp.callback_query_handler(lambda c: c.data == "cancel_manage", state=ManageCabinetStates.all_states)
async def cancel_manage_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await show_main_menu(callback.message.chat.id)
    await callback.message.delete()

@dp.callback_query_handler(lambda c: c.data == "rename_cabinet", state=ManageCabinetStates.ACTION_CHOICE)
async def rename_cabinet_callback(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.message.delete()
        # bot.delete_message(
        #     chat_id=callback.message.chat.id,
        #     message_id=callback.message.message_id)
    except:
        pass
    await callback.message.answer("Введите новое название для кабинета:", reply_markup=get_cancel_keyboard())
    await ManageCabinetStates.WAITING_NEW_NAME.set()

@dp.message_handler(state=ManageCabinetStates.WAITING_NEW_NAME)
async def process_new_cabinet_name(message: types.Message, state: FSMContext):
    new_name = message.text.strip()
    user_id = message.from_user.id
    if is_admin(user_id):
        return

    if not validate_cabinet_name(new_name):
        await message.answer("❌ Название кабинета должно быть от 2 до 50 символов!")
        return

    async with state.proxy() as data:
        old_name = data['cabinet']
        username = data['username']

    wait_message = await message.answer("🔄 Ожидайте 30 сек, идёт переименование кабинета", reply_markup=main_menu_keyboard)
    success = await run_in_thread(update_cabinet_name, username, old_name, new_name)
    if success:
        await message.answer(f"✅ Кабинет успешно переименован: {old_name} → {new_name}")
        cache.config_cache = None
    else:
        await message.answer("❌ Ошибка при переименовании кабинета")
    await state.finish()
    await show_main_menu(message.chat.id)
    try:
        await bot.delete_message(message.chat.id, wait_message.message_id)
    except:
        pass

def update_cabinet_name(username: str, old_name: str, new_name: str) -> bool:
    try:
        worksheet = gc.open_by_key(CONFIG_SHEET_ID).sheet1
        records = worksheet.get_all_values()
        for i, row in enumerate(records):
            if row[0] == username and row[2] == old_name:
                worksheet.update_cell(i+1, 3, new_name)
        
        spreadsheet_url = cache.user_spreadsheet_urls.get(username)
        spreadsheet = gc.open_by_url(spreadsheet_url)
        # worksheet_user = spreadsheet.get_worksheet(0)
        worksheet_user = spreadsheet.worksheet("Маржа")
        
        # Получаем все данные за один запрос
        all_values = worksheet_user.get_all_values()
        
        for i in range(len(all_values)):
            if all_values[i][0] == old_name:
                all_values[i][0] = new_name
        
        # Обновляем весь лист за один запрос
        worksheet_user.update(all_values, 'A1')
        return True
    except Exception as e:
        logging.error(f"Ошибка переименования кабинета: {e}")
        return False

@dp.callback_query_handler(lambda c: c.data == "delete_cabinet", state=ManageCabinetStates.ACTION_CHOICE)
async def delete_cabinet_callback(callback: types.CallbackQuery, state: FSMContext):

    if is_admin(callback.from_user.id):
        return
    async with state.proxy() as data:
        cabinet_name = data['cabinet']
        username = data['username']

    try:
        wait_message = await callback.message.edit_text(
            text="🔄 Ожидайте 30 сек, идёт удаление кабинета...",
            reply_markup=None  # Убираем клавиатуру
        )
    except Exception as e:
        logging.error(f"Ошибка редактирования сообщения: {e}")
        # Если не удалось отредактировать, отправляем новое сообщение    
        wait_message = await callback.message.answer("🔄 Ожидайте 30 сек, идёт удаление кабинета...", reply_markup=main_menu_keyboard)
    
    success = await run_in_thread(delete_cabinet, username, cabinet_name)
    if success:
        await callback.message.answer(f"✅ Кабинет '{cabinet_name}' успешно удалён")
        cache.config_cache = None
    else:
        await callback.message.answer(f"❌ Ошибка при удалении кабинета '{cabinet_name}'")
    await state.finish()
    await show_main_menu(callback.message.chat.id)
    await state.finish()
    try:
        await bot.delete_message(callback.message.chat.id, wait_message.message_id)
    except:
        pass

def delete_cabinet(username: str, cabinet_name: str) -> bool:
    try:
        worksheet = gc.open_by_key(CONFIG_SHEET_ID).sheet1
        records = worksheet.get_all_values()
        row_to_delete = None
        for i, row in enumerate(records):
            if row[0] == username and row[2] == cabinet_name:
                row_to_delete = i+1
                break
        if not row_to_delete:
            return False
        worksheet.delete_rows(row_to_delete)
        
        # Удаляем артикулы кабинета из таблицы пользователя
        spreadsheet_url = cache.user_spreadsheet_urls.get(username)
        if spreadsheet_url:
            try:
                spreadsheet = gc.open_by_url(spreadsheet_url)
                # worksheet_user = spreadsheet.get_worksheet(0)
                worksheet_user = spreadsheet.worksheet("Маржа")
                
                # Получаем все данные за один запрос
                all_values = worksheet_user.get_all_values()
                
                instruction_row = all_values[0]
                
                header_row = all_values[2]
                data = all_values[3:]  # Данные начинаются с 4-й строки

                worksheet_user.clear()

                # Восстанавливаем структуру
                worksheet_user.append_row(instruction_row)
                worksheet_user.append_row(header_row, table_range='A3:E3')
                
                new_values = [
                    row for row in data 
                    if not row or row[0] != cabinet_name  # Проверяем первый столбец (название кабинета)
                ]
                
                worksheet_user.append_rows(new_values)

                # Восстанавливаем форматирование
                worksheet_user.format("A1", {
                    "textFormat": {
                        "bold": True,
                        "fontSize": 14
                    },
                    "horizontalAlignment": "LEFT",
                    "wrapStrategy": "WRAP"
                })
                worksheet_user.merge_cells("A1:E1")
                
                # Серый цвет для заголовков (строка 3)
                worksheet_user.format("A3:E3", {
                    "backgroundColor": {
                        "red": 0.9,
                        "green": 0.9,
                        "blue": 0.9
                    },
                    "textFormat": {
                        "bold": True
                    }
                })
                
            except Exception as e:
                logging.error(f"Ошибка при удалении артикулов кабинета {cabinet_name} из таблицы пользователя: {e}")
        
        return True
    except Exception as e:
        logging.error(f"Ошибка удаления кабинета: {e}")
        return False

@dp.callback_query_handler(lambda c: c.data == "refresh_articles", state=ManageCabinetStates.ACTION_CHOICE)
async def refresh_articles_callback(callback: types.CallbackQuery, state: FSMContext):

    if is_admin(callback.from_user.id):
        return
    async with state.proxy() as data:
        cabinet_name = data['cabinet']
        username = data['username']

    api_key = await run_in_thread(get_cabinet_api_key, username, cabinet_name)
    if not api_key:
        await callback.answer("❌ Не удалось получить API ключ для кабинета")
        await state.finish()
        await show_main_menu(callback.message.chat.id)
        return

    spreadsheet_url = cache.user_spreadsheet_urls.get(username)
    if not spreadsheet_url:
        await callback.answer("❌ Не удалось найти таблицу пользователя")
        await state.finish()
        await show_main_menu(callback.message.chat.id)
        return

    await callback.answer()
    try:
        msg = await callback.message.edit_text(
            text="⏳ Ожидайте 30 секунд, идёт обработка...",
            reply_markup=None  # Убираем клавиатуру
        )
    except Exception as e:
        logging.error(f"Ошибка редактирования сообщения: {e}")
        # Если не удалось отредактировать, отправляем новое сообщение    
        msg = await bot.send_message(callback.from_user.id, "⏳ Ожидайте 30 секунд, идёт обработка...", reply_markup=main_menu_keyboard)
    try:
        spreadsheet = gc.open_by_url(spreadsheet_url)
        # worksheet = spreadsheet.get_worksheet(0)
        worksheet = spreadsheet.worksheet("Маржа")
        existing_pairs = get_actual_articles(worksheet)
        new_pairs = set(get_wb_articles(api_key))
        new_pairs_with_cabinet = set([(cabinet_name, nmId, supplierArticle)
                                      for (nmId, supplierArticle) in new_pairs])
        missing_pairs = list(new_pairs_with_cabinet - existing_pairs)
        if missing_pairs:
            await run_in_thread(add_articles_to_sheet, worksheet, missing_pairs)
            await bot.send_message(callback.from_user.id, f"✅ Добавлено {len(missing_pairs)} новых пар артикулов и баркодов!")
        else:
            await bot.send_message(callback.from_user.id, "ℹ️ Все артикулы и баркоды уже актуальны!")
        try:
            await bot.delete_message(callback.message.chat.id, msg.message_id)
        except:
            pass
    except Exception as e:
        logging.error(f"Ошибка обновления артикулов: {e}")
        await bot.send_message(callback.from_user.id, "❌ Ошибка при обновлении артикулов")
    await state.finish()
    await show_main_menu(callback.message.chat.id)

def get_actual_articles(worksheet):
    existing_pairs = set()
    # Пропускаем первые 3 строки (инструкция, пустая, заголовки)
    records = worksheet.get_all_values()[3:]
    for row in records:
        if len(row) >= 5:
            cabinet = str(row[0]).strip()
            nmId = str(row[1]).strip()
            article = str(row[2]).strip()
            if cabinet and nmId and article:
                existing_pairs.add((cabinet, nmId, article))
    return existing_pairs

async def on_startup(dp):
    await cache.load_data()
    await cache.update_config_cache()
    
    # Удаляем старую задачу перед добавлением
    try:
        scheduler.remove_job("daily_config_update")
    except:
        pass

    scheduler.add_job(
        main_from_config,
        'cron',
        hour=0,
        minute=30,
        timezone=MOSCOW_TZ,
        args=[CONFIG_URL],
        id="daily_config_update"
    )
    scheduler.start()

async def on_shutdown(dp):
    scheduler.shutdown()

@dp.callback_query_handler(lambda c: c.data == "faq")
async def faq_callback(callback: types.CallbackQuery):
    await callback.answer("📚 Раздел с ответами на вопросы в разработке", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "support")
async def support_callback(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.message.answer(
        "✍️ Опишите ваш вопрос или проблему. Администратор ответит вам в ближайшее время.",
        reply_markup=get_cancel_keyboard()
    )
    await SupportStates.WAITING_QUESTION.set()
    await callback.answer()

@dp.message_handler(state=SupportStates.WAITING_QUESTION)
async def process_support_question(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or "без username"
    question = message.text

    async with state.proxy() as data:
        data['question'] = question

    for admin_id in ADMIN_IDS:
        try:
            text = (
                f"🆘 Новый вопрос в поддержку\n"
                f"• Пользователь: @{username} ({user_id})\n"
                f"• Вопрос: {question}"
            )
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton(
                text="✍️ Ответить",
                callback_data=f"reply_to:{user_id}"
            ))
            await bot.send_message(admin_id, text, reply_markup=kb)
        except Exception as e:
            logging.error(f"Не удалось отправить вопрос админу {admin_id}: {e}")

    await message.answer("✅ Ваш вопрос отправлен в поддержку. Ожидайте ответа.")
    await state.finish()
    await show_main_menu(message.chat.id)

@dp.callback_query_handler(lambda c: c.data.startswith("reply_to:"))
async def reply_to_user_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id_to_reply = int(callback.data.split(":")[1])
    async with state.proxy() as data:
        data['user_id_to_reply'] = user_id_to_reply
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_manage"))
    await callback.message.answer(
        f"✍️ Введите ответ для пользователя (ID: {user_id_to_reply}):",
        reply_markup=get_cancel_keyboard()
    )
    await SupportStates.WAITING_REPLY.set()
    await callback.answer()

@dp.message_handler(state=SupportStates.WAITING_REPLY)
async def process_support_reply(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        user_id_to_reply = data['user_id_to_reply']
        reply_text = message.text

    try:
        await bot.send_message(
            user_id_to_reply,
            f"📩 Ответ от поддержки:\n\n{reply_text}"
        )
        await message.answer(f"✅ Ответ отправлен пользователю {user_id_to_reply}")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить сообщение: {str(e)}")
    await state.finish()

# Обработчик кнопки "Главное меню"
@dp.message_handler(lambda message: message.text == "Главное меню", state="*")
async def main_menu_button_handler(message: types.Message, state: FSMContext):
    if is_admin(message.from_user.id):
        return
    current_state = await state.get_state()
    if current_state:
        await state.finish()
    await show_main_menu(message.chat.id)

# Обработчик кнопки "Рассылка"
@dp.callback_query_handler(lambda c: c.data == "admin_broadcast")
async def broadcast_callback(callback: types.CallbackQuery):
    if is_admin(callback.from_user.id):
        await callback.message.delete()
        await callback.message.answer(
            "✍️ Введите сообщение для рассылки всем пользователям:",
            reply_markup=get_cancel_admin_keyboard()
        )
        await BroadcastStates.WAITING_MESSAGE.set()
    await callback.answer()

# Обработчик текста сообщения для рассылки
@dp.message_handler(state=BroadcastStates.WAITING_MESSAGE)
async def process_broadcast_message(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        data['message'] = message.text
    
    # Создаем клавиатуру подтверждения
    confirm_kb = InlineKeyboardMarkup(row_width=2)
    confirm_kb.add(
        InlineKeyboardButton("✅ Отправить", callback_data="confirm_broadcast"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_broadcast")
    )
    
    await message.answer(
        f"✉️ Сообщение для рассылки:\n\n{message.text}\n\n"
        "Отправить всем пользователям?",
        reply_markup=confirm_kb
    )
    await BroadcastStates.CONFIRMATION.set()

# Обработчик подтверждения рассылки
@dp.callback_query_handler(lambda c: c.data == "confirm_broadcast", state=BroadcastStates.CONFIRMATION)
async def confirm_broadcast(callback: types.CallbackQuery, state: FSMContext):
    admin_id = callback.from_user.id
    async with state.proxy() as data:
        message_text = data['message']
    
    # Получаем всех пользователей
    users = cache.user_mapping.keys()
    total = len(users)
    success = 0
    failed = 0
    
    # Отправляем сообщение с индикатором прогресса

    status_msg = await bot.send_message(admin_id, f"🔄 Начата рассылка... 0/{total}")
    
    # Рассылаем сообщения
    for i, user_id in enumerate(users):
        try:
            await bot.send_message(user_id, message_text)
            success += 1
        except Exception as e:
            logging.error(f"Ошибка отправки сообщения пользователю {user_id}: {e}")
            failed += 1
        
        # Обновляем статус каждые 10 сообщений
        if i % 10 == 0:
            try:
                await status_msg.edit_text(
                    f"🔄 Рассылка... {i+1}/{total}\n"
                    f"✅ Успешно: {success}\n"
                    f"❌ Ошибки: {failed}"
                )
            except:
                await callback.answer()
    
    # Отправляем финальный отчет
    await bot.send_message(
        admin_id,
        f"📢 Рассылка завершена!\n"
        f"• Всего пользователей: {total}\n"
        f"• Успешно отправлено: {success}\n"
        f"• Не удалось отправить: {failed}"
    )
    
    try:
        await bot.delete_message(admin_id, status_msg.message_id)
    except:
        pass
    await show_admin_menu(callback.message.chat.id)
    await state.finish()

# Обработчик отмены рассылки
@dp.callback_query_handler(lambda c: c.data == "cancel_broadcast", state=BroadcastStates.CONFIRMATION)
async def cancel_broadcast(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await show_admin_menu(callback.message.chat.id)
    await callback.message.delete()


async def main():
    await on_startup(dp)
    try:
        await dp.start_polling()
    finally:
        await on_shutdown(dp)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    asyncio.run(main())
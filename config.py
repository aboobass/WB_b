import pytz
from datetime import time
from google.oauth2.service_account import Credentials
# исходная таблица
CONFIG_URL = "https://docs.google.com/spreadsheets/d/14yawg6EXbXus8R3Q9JvaZA4eQboX1vdN1GEFzZzwKJs/edit#gid=0"
CONFIG_SHEET_ID = '14yawg6EXbXus8R3Q9JvaZA4eQboX1vdN1GEFzZzwKJs'
# токен бота
API_TOKEN = "7765052912:AAFeX7RxFtRy2hrnlWoaFn-eTgMp5th5pGk"
# ADMIN_IDS = []  # ID админов через запятую
ADMIN_IDS = [784291592]  # ID админов через запятую

NOTIFY_ADMINS_ON_NEW_USER = True  # Включить/выключить уведомления


SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
CREDS = Credentials.from_service_account_file(
    'credentials.json', scopes=SCOPES)


# Настройки времени
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Время отчетов по умолчанию (6:00 МСК)
DEFAULT_TIME = time(6, 0, tzinfo=MOSCOW_TZ)

# Файл для сохранения данных
DATA_FILE = "user_data.json"

# Добавляем константы для оплаты
SUBSCRIPTION_PRICE = 780

PAYMENT_PROVIDER_TOKEN = "390540012:LIVE:74012"
PAYMENT_TITLE = "Подписка на бота ПРИБЫЛЬ СЕЙЧАС | WB"
PAYMENT_DESCRIPTION = "Доступ к функционалу бота на 1 месяц"


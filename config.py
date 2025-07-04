from google.oauth2.service_account import Credentials
# исходная таблица
CONFIG_URL = "https://docs.google.com/spreadsheets/d/14yawg6EXbXus8R3Q9JvaZA4eQboX1vdN1GEFzZzwKJs/edit#gid=0"
CONFIG_SHEET_ID = '14yawg6EXbXus8R3Q9JvaZA4eQboX1vdN1GEFzZzwKJs'
# токен бота
# API_TOKEN = "8194459741:AAGx3YOGtQy96ZAJJiou9u4sUewLHIXScmQ"
API_TOKEN = "7765052912:AAFeX7RxFtRy2hrnlWoaFn-eTgMp5th5pGk"
ADMIN_IDS = [784291592]  # ID админов через запятую

NOTIFY_ADMINS_ON_NEW_USER = True  # Включить/выключить уведомления


SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
CREDS = Credentials.from_service_account_file(
    'credentials.json', scopes=SCOPES)

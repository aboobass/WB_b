import gspread
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from config import CONFIG_URL, CREDS
from WB_ads import get_expenses_per_nm
from WB_orders import get_dict_orders, get_wb_product_cards
import numpy as np
import logging

# Настройки WB API
WB_STAT_URL = 'https://statistics-api.wildberries.ru/api/v1/supplier/'
HEADERS = {}

# Глобальная переменная для API ключа WB
WB_API_KEY = ""


async def get_client_data(sheet_id):
    try:
        client = gspread.authorize(CREDS)
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet("Маржа")

        # Получаем все данные за один запрос
        records = worksheet.get_all_records(head=3,
                                            value_render_option='UNFORMATTED_VALUE',
                                            expected_headers=["Артикул WB", "Артикул продавца", "Прибыль с ед. товара",	"Выкупаемость (%)"])

        # Создаем словарь для быстрого поиска по артикулу
        data_dict = {}

        for row in records:
            nmId = int(str(row.get('Артикул WB')).strip())
            if nmId:  # Пропускаем пустые значения
                data_dict[nmId] = {
                    'vendorCode': row.get('Артикул продавца', ''),
                    'profit': row.get('Прибыль с ед. товара', ''),
                    'redemption': row.get('Выкупаемость (%)', '')
                }
        return data_dict
    except Exception as e:
        print(f"Ошибка при получении данных из таблицы {sheet_id}: {e}")
        return {}

async def calculate_metrics(orders, ad_stats_df, client_sheet_id=None):
    try:
        # Получаем все данные из таблицы клиента одним запросом
        client_data = {}
        if client_sheet_id:
            client_data = await get_client_data(client_sheet_id)

        for nmId in orders.keys():
            if nmId in ad_stats_df:
                orders[nmId]['costs'] = round(ad_stats_df[nmId]['sum'], 2)
                orders[nmId]['views'] = round(ad_stats_df[nmId]['views'], 2)
                
                if ad_stats_df[nmId]['auto_views'] != 0:
                    orders[nmId]['auto_ctr'] = round(ad_stats_df[nmId]['auto_clicks'] * 100 / ad_stats_df[nmId]['auto_views'], 2)                
                else:
                    orders[nmId]['auto_ctr'] = 0.0

                if ad_stats_df[nmId]['auction_views'] != 0:
                    orders[nmId]['auction_ctr'] = round(ad_stats_df[nmId]['auction_clicks'] * 100 / ad_stats_df[nmId]['auction_views'], 2)
                else:
                    orders[nmId]['auction_ctr'] = 0.0
            else:
                orders[nmId]['costs'] = 0.0
                orders[nmId]['views'] = 0.0
                orders[nmId]['auto_ctr'] = 0.0
                orders[nmId]['auction_ctr'] = 0.0

            if nmId in client_data:
                if client_data[nmId]['profit'] != '' and client_data[nmId]['redemption'] != '':
                    orders[nmId]['redemption_rate'] = float(client_data[nmId]['redemption']) / 100
                    orders[nmId]['profit_per_unit'] = float(client_data[nmId]['profit'])

                    # Расчёт прибыли
                    orders[nmId]['gross_profit'] = round(orders[nmId]['profit_per_unit'] * orders[nmId]['ordersCount'] * orders[nmId]['redemption_rate'], 2)
                else:
                    orders[nmId]['redemption_rate'] = None
                    orders[nmId]['profit_per_unit'] = None
                    orders[nmId]['gross_profit']= None
            else:
                orders[nmId]['redemption_rate'] = None
                orders[nmId]['profit_per_unit'] = None
                orders[nmId]['gross_profit']= None
        
        result = []
        for nmId, values in orders.items():
            # print(nmId, values)
            if values['ordersCount'] != 0:
                result.append([nmId, client_data[nmId]['vendorCode'], values['ordersCount'], values['costs'], values['gross_profit'], values['ordersSumRub'], ' ', values['views'], values['auto_ctr'], values['auction_ctr'], values.get('addToCartConversion', 0), values.get('cartToOrderConversion', 0)])
        result = pd.DataFrame(result, columns=['nmId', 'vendorCode', 'ordersCount', 'costs', 'gross_profit', 'ordersSumRub', 'void', 'views', 'auto_ctr', 'auction_ctr', 'addToCartConversion', 'cartToOrderConversion'])
        for index, row in result.iterrows():
            if pd.notna(result.at[index, 'costs']) and pd.notna(result.at[index, 'gross_profit']):
                result.at[index, 'net_profit'] = (result.at[index, 'gross_profit'] -
                                                  result.at[index, 'costs']).round(2)
            else:
                # print(index, result.at[index, 'gross_profit'], result.at[index, 'costs'])
                result.at[index, 'net_profit'] = None
            if pd.notna(result.at[index, 'costs']) and pd.notna(result.at[index, 'ordersSumRub']):
                result.at[index, 'drr'] = (result.at[index, 'costs'] /
                                           result.at[index, 'ordersSumRub'] * 100).round(2)
            else:
                result.at[index, 'drr'] = 0
        result = result.rename(columns={
            'nmId': 'Артикул WB',
            'vendorCode': 'Артикул продавца',
            'ordersCount': 'Количество заказов за период',
            'costs': 'Расходы на рекламу по артикулу',
            'ordersSumRub': 'Сумма заказов',
            'drr': 'ДРР',
            'net_profit': 'Чистая прибыль за период по артикулу',
            'void': ' ',
            'views': 'Показы',
            'auto_ctr': 'CTR (Автоматические компании)',
            'auction_ctr': 'CTR (Аукционы)',
            'addToCartConversion': 'Конверсия в корзину',
            'cartToOrderConversion': 'Конверсия в заказ'
        })

        result['Дата'] = (datetime.now() - timedelta(days=1)
                          ).strftime('%d.%m.%Y')
        # Заменяем None на строку "Нет данных"
        for col in ['Чистая прибыль за период по артикулу']:
            result[col] = result[col].apply(
                lambda x: "ВНЕСИТЕ" if pd.isna(x) or x is None else x
            )

        return result[[
            'Дата', 'Артикул WB', 'Артикул продавца', 'Количество заказов за период',
            'Расходы на рекламу по артикулу', 'Сумма заказов', 'ДРР', 'Чистая прибыль за период по артикулу', ' ', 'Показы', 
            'CTR (Автоматические компании)', 'CTR (Аукционы)', 'Конверсия в корзину', 'Конверсия в заказ', 
        ]]
    except Exception as e:
        print(f"Ошибка при расчете метрик: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

async def read_config(config_sheet_url: str) -> dict:
    """Чтение конфигурации: {user: [(sheet_id, wb_api_key, sheet_name)]}"""
    client = gspread.authorize(CREDS)
    sheet = client.open_by_url(config_sheet_url).sheet1
    records = sheet.get_all_records(
        expected_headers=["Клиент", "WB ключ", "Личный кабинет", "Ссылка на таблицу"])
    result = {}
    for row in records:
        link = row.get("Ссылка на таблицу")
        wb_key = row.get("WB ключ")
        sheet_name = row.get("Личный кабинет")
        user = row.get("Клиент")
        if link and wb_key and sheet_name and user:
            # Извлекаем ID таблицы из ссылки
            sheet_id = link.split("/d/")[1].split("/")[0]
            if user not in result:
                result[user] = []
            result[user].append((sheet_id, wb_key.strip(), sheet_name.strip()))
    return result

def get_available_users_from_config(config_url: str) -> list:
    """Возвращает список доступных пользователей из конфигурации"""
    try:
        client = gspread.authorize(CREDS)
        sheet = client.open_by_url(config_url).sheet1
        records = sheet.get_all_records(
            expected_headers=["Клиент"])

        users = set()
        for row in records:
            if row.get("Клиент"):
                users.add(str(row["Клиент"]).strip())
        return sorted(users)
    except Exception as e:
        print(f"Ошибка при получении списка пользователей: {e}")
        return []

def get_user_cabinets(config_url: str, username: str) -> list:
    """Возвращает личные кабинеты для конкретного пользователя"""
    try:
        client = gspread.authorize(CREDS)
        sheet = client.open_by_url(config_url).sheet1
        records = sheet.get_all_records(
            expected_headers=["Клиент", "WB ключ", "Личный кабинет", "Ссылка на таблицу"])
        cabinets = []
        for row in records:
            if str(row.get("Клиент", "")).strip() == username and row.get("Личный кабинет"):
                cabinets.append(str(row["Личный кабинет"]).strip())
        return sorted(cabinets)
    except Exception as e:
        print(f"Ошибка при получении кабинетов пользователя: {e}")
        return []

async def update_google_sheet_multi(sheet_id, sheet_name, data_df, spreadsheet):
    if data_df.empty:
        print(f"[{sheet_name}] Нет данных для записи")
        return

    try:
        num_columns = len(data_df.columns)
        
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            is_new_sheet = False
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name, rows="100", cols=15)
            is_new_sheet = True

        # Проверка и расширение столбцов при необходимости
        current_col_count = worksheet.col_count
        if num_columns > current_col_count:
            cols_to_add = num_columns - current_col_count
            worksheet.add_cols(cols_to_add)
        
        last_col_letter = gspread.utils.rowcol_to_a1(1, num_columns)[0]

        if is_new_sheet:
            # Шапка для нового листа
            worksheet.append_row(["Ежедневная статистика"])
            worksheet.append_row(["Таблица обновляется ежедневно с 00:00 до 01:00"])
            
            # Форматирование шапки
            worksheet.format('A1:A2', {
                "textFormat": {"bold": True, "fontSize": 14},
                "horizontalAlignment": "CENTER"
            })
            
            # Объединение ячеек
            worksheet.merge_cells(f'A1:{last_col_letter}1')
            worksheet.merge_cells(f'A2:{last_col_letter}2')
            
            # Отступ и заголовки столбцов
            worksheet.insert_row([""], index=3)
            lst_headers = list(data_df.columns)
            # print(lst_headers)
            # lst_headers.insert(8, ' ')
            # print(lst_headers)
            worksheet.insert_row(lst_headers, index=4)
            
            # Форматирование заголовков
            header_format = {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                "borders": {"top": {"style": "SOLID"}, "bottom": {"style": "SOLID"}, 
                            "left": {"style": "SOLID"}, "right": {"style": "SOLID"}},
                "wrapStrategy": "WRAP"
            }
            worksheet.format(f'A4:{last_col_letter}4', header_format)
            # Сброс только самых важных параметров
            worksheet.format("I4", {
                "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},  # белый фон
                "textFormat": {"bold": False},  # обычный шрифт
                "borders": {}  # без границ
            })
            worksheet.freeze(4)

        # Подготовка данных
        values = []
        for _, row in data_df.iterrows():
            converted_row = []
            for item in row:
                if isinstance(item, np.integer):
                    converted_row.append(int(item))
                elif isinstance(item, np.floating):
                    converted_row.append(float(item))
                else:
                    converted_row.append(item)
            values.append(converted_row)

        # Расчет итогов - ИСПРАВЛЕННЫЙ БЛОК
        total_orders = total_revenue = total_costs = total_net_profit = 0
        col_index_map = {col: idx for idx, col in enumerate(data_df.columns)}
        
        if 'Количество заказов за период' in data_df.columns:
            # Используем sum() с skipna=True для числовых столбцов
            total_orders = int(data_df['Количество заказов за период'].sum(skipna=True))
            
        if 'Сумма заказов' in data_df.columns:
            total_revenue = float(data_df['Сумма заказов'].sum(skipna=True))
            
        if 'Расходы на рекламу по артикулу' in data_df.columns:
            # Обрабатываем только числовые значения, игнорируем строки
            total_costs = data_df['Расходы на рекламу по артикулу'].apply(
                lambda x: float(str(x).replace(',', '.')) if isinstance(x, (int, float, str)) and str(x).replace(',', '').replace('.', '').isdigit() else 0
            ).sum()
            
        if 'Чистая прибыль за период по артикулу' in data_df.columns:
            # Обрабатываем только числовые значения, игнорируем строки
            total_net_profit = data_df['Чистая прибыль за период по артикулу'].apply(
                lambda x: x if isinstance(x, (int, float)) else 0
            ).sum()
        
        # Формирование итоговой строки
        total_row = [''] * num_columns
        total_row[0] = 'Итого'
        if 'Количество заказов за период' in col_index_map:
            total_row[col_index_map['Количество заказов за период']] = int(total_orders)
        if 'Сумма заказов' in col_index_map:
            total_row[col_index_map['Сумма заказов']] = float(total_revenue)
        if 'Расходы на рекламу по артикулу' in col_index_map:
            total_row[col_index_map['Расходы на рекламу по артикулу']] = float(total_costs)
        if 'Чистая прибыль за период по артикулу' in col_index_map:
            total_row[col_index_map['Чистая прибыль за период по артикулу']] = float(total_net_profit)
        
        all_values = values + [total_row]

        # Определение стартовой строки
        if is_new_sheet:
            start_row = 5
        else:
            all_values_in_sheet = worksheet.get_all_values()
            start_row = len(all_values_in_sheet) + 1
        
        # Проверка и расширение строк
        current_row_count = worksheet.row_count
        last_row_needed = start_row + len(all_values)
        if last_row_needed > current_row_count:
            worksheet.add_rows(last_row_needed - current_row_count)

        # Запись данных
        update_range = f"A{start_row}:{last_col_letter}{start_row + len(all_values) - 1}"
        worksheet.update(range_name=update_range, values=all_values)

        # Форматирование строки "Итого"
        total_row_position = start_row + len(values)
        total_row_range = f"A{total_row_position}:{last_col_letter}{total_row_position}"
        
        worksheet.format(total_row_range, {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}
        })

        print(f"[{sheet_name}] Добавлено строк: {len(values)}")
    except Exception as e:
        print(f"[{sheet_name}] Ошибка при обновлении Google Таблицы: {e}")
        import traceback
        traceback.print_exc()

async def main_from_config(config_url: str, date_from=None, date_to=None):
    if not date_from:
        date_from = datetime.now() - timedelta(days=1)  # Минус 1 день
        date_from = date_from.replace(hour=0, minute=0, second=0,
                                      microsecond=0).strftime('%Y-%m-%dT%H:%M:%S')
    if not date_to:
        date_to = datetime.now().replace(hour=0, minute=0, second=0,
                                         microsecond=0).strftime('%Y-%m-%dT%H:%M:%S')

    print(f"\nОбработка данных за период: {date_from} - {date_to}")

    try:
        configs = await read_config(config_url)
        for user, user_configs in configs.items():
            print(f"\n--- Обработка пользователя: {user} ---")
            client = gspread.authorize(CREDS)
            spreadsheet = client.open_by_key(user_configs[0][0])

            for sheet_id, wb_key, sheet_name in user_configs:
                print(f"\n--- Обработка ЛК: {sheet_name} ---")

                global WB_API_KEY, HEADERS
                WB_API_KEY = wb_key
                HEADERS = {'Authorization': WB_API_KEY}
                # Получение данных с возобновляемой обработкой
                orders = None
                orders_state = None
                max_retries = 10
                cards = await get_wb_product_cards(HEADERS)
                for attempt in range(max_retries):
                    orders = await get_dict_orders(HEADERS, date_from[:10], state=orders_state, cards=cards)
                    
                    # Если получили состояние для повтора
                    if isinstance(orders, dict) and orders.get('error') == 429:
                        wait_time = 30
                        logging.info(f"Waiting {wait_time}s for orders API (attempt {attempt+1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        orders_state = orders.get('state')
                        continue
                        
                    # Успешное завершение
                    break
                
                # Если после всех попыток всё равно ошибка
                if isinstance(orders, dict) and orders.get('error') == 429:
                    return pd.DataFrame(), "429_error"
                
                # Получение расходов на рекламу
                ad_stats = None
                # ad_state = None
                max_retries = 3
                for attempt in range(max_retries):
                    ad_stats = await get_expenses_per_nm(HEADERS, date_from)
                    
                    if isinstance(ad_stats, dict) and ad_stats.get('error') == 429:
                        wait_time = 30
                        logging.info(f"Waiting {wait_time}s for ads API (attempt {attempt+1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                        
                    break
                
                if isinstance(ad_stats, dict) and ad_stats.get('error') == 429:
                    return pd.DataFrame(), "429_error"
                metrics_df = await calculate_metrics(
                    orders, ad_stats, sheet_id)

                if not metrics_df.empty:
                    await update_google_sheet_multi(
                        sheet_id, sheet_name, metrics_df, spreadsheet)
                else:
                    print(f"[{sheet_name}] Нет данных для записи")

    except Exception as e:
        print(f"Критическая ошибка: {e}")

async def calculate_metrics_for_bot(orders, ad_stats_df, client_sheet_id=None):
    try:
        # Получаем все данные из таблицы клиента одним запросом
        client_data = {}
        if client_sheet_id:
            client_data = await get_client_data(client_sheet_id)

        for nmId in orders.keys():
            if nmId in ad_stats_df:
                orders[nmId]['costs'] = round(ad_stats_df[nmId]['sum'], 2)
                # orders[nmId]['views'] = round(ad_stats_df[nmId]['views'], 2)
                # orders[nmId]['auto_ctr'] = round(ad_stats_df[nmId]['auto_ctr'], 2)
                # orders[nmId]['auction_ctr'] = round(ad_stats_df[nmId]['auction_ctr'], 2)
            else:
                orders[nmId]['costs'] = 0 
            
            if nmId in client_data:
                if client_data[nmId]['profit'] != '' and client_data[nmId]['redemption'] != '':
                    orders[nmId]['redemption_rate'] = float(client_data[nmId]['redemption']) / 100
                    orders[nmId]['profit_per_unit'] = float(client_data[nmId]['profit'])

                    # Расчёт прибыли
                    orders[nmId]['gross_profit'] = round(orders[nmId]['profit_per_unit'] * orders[nmId]['ordersCount'] * orders[nmId]['redemption_rate'], 2)
                else:
                    orders[nmId]['redemption_rate'] = None
                    orders[nmId]['profit_per_unit'] = None
                    orders[nmId]['gross_profit']= None
            else:
                orders[nmId]['redemption_rate'] = None
                orders[nmId]['profit_per_unit'] = None
                orders[nmId]['gross_profit']= None
        result = []
        for nmId, values in orders.items():
            if values['ordersCount'] != 0:
                result.append([nmId, client_data[nmId]['vendorCode'], values['ordersCount'], values['costs'], values['gross_profit'], values['ordersSumRub']])
        result = pd.DataFrame(result, columns=['nmId', 'vendorCode', 'ordersCount', 'costs', 'gross_profit', 'ordersSumRub'])


        for index, row in result.iterrows():
            if pd.notna(result.at[index, 'costs']) and pd.notna(result.at[index, 'gross_profit']):            
                result.at[index, 'net_profit'] = (result.at[index, 'gross_profit'] -
                                                  result.at[index, 'costs']).round(2)
            else:
                result.at[index, 'net_profit'] = None

        result = result.rename(columns={
            'nmId': 'Артикул WB',
            'vendorCode': 'Артикул продавца',
            'ordersCount': 'Кол-во заказов',
            'costs': 'Расходы РК',
            'net_profit': 'Прибыль'
        })
        return result[[
            'Артикул WB',
            'Артикул продавца',
            'Кол-во заказов',
            'Расходы РК',
            'Прибыль'
        ]]
    except Exception as e:
        print(f"Ошибка при расчете метрик: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

async def generate_summary(df):
    """Генерирует краткую сводку по отчету"""
    if df.empty:
        return "Нет данных для формирования сводки"

    try:
        # print(df)
        total_profit = df['Прибыль'].sum()
        if pd.isna(total_profit):
            total_profit = 0
        total_ads = df['Расходы РК'].sum()
        total_orders = df['Кол-во заказов'].sum()

        summary = [
            f"{int(total_orders)}:{total_ads: .2f}:{total_profit: .2f}"
        ]

        return "\n".join(summary)
    except Exception as e:
        print(f"Ошибка при формировании сводки: {e}")
        return "Не удалось сформировать сводку"

async def generate_report(sheet_user: str, sheet_name: str, config_url: str, date_from=None, date_to=None) -> tuple:
    if not date_from:
        date_from = datetime.now().replace(hour=0, minute=0, second=0,
                                           microsecond=0).strftime('%Y-%m-%dT%H:%M:%S')
    if not date_to:
        date_to = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    try:
        configs = await read_config(config_url)
        for user, user_configs in configs.items():
            for sheet_id, wb_key, config_sheet_name in user_configs:
                if config_sheet_name == sheet_name and user == sheet_user:
                    global WB_API_KEY, HEADERS
                    WB_API_KEY = wb_key
                    HEADERS = {'Authorization': WB_API_KEY}
                    
                    # Получение данных с возобновляемой обработкой
                    orders = None
                    orders_state = None
                    max_retries = 10
                    cards = await get_wb_product_cards(HEADERS)
                    for attempt in range(max_retries):
                        orders = await get_dict_orders(HEADERS, date_from[:10], state=orders_state, cards=cards)
                        
                        # Если получили состояние для повтора
                        if isinstance(orders, dict) and orders.get('error') == 429:
                            wait_time = 30
                            logging.info(f"Waiting {wait_time}s for orders API (attempt {attempt+1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                            orders_state = orders.get('state')
                            continue
                            
                        # Успешное завершение
                        break
                    
                    # Если после всех попыток всё равно ошибка
                    if isinstance(orders, dict) and orders.get('error') == 429:
                        return pd.DataFrame(), "429_error"
                    
                    max_retries = 3
                    # Получение расходов на рекламу
                    ad_stats = None
                    ad_state = None
                    for attempt in range(max_retries):
                        ad_stats = await get_expenses_per_nm(HEADERS, date_from)
                        
                        if isinstance(ad_stats, dict) and ad_stats.get('error') == 429:
                            wait_time = 30
                            logging.info(f"Waiting {wait_time}s for ads API (attempt {attempt+1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                            continue
                            
                        break
                    
                    if isinstance(ad_stats, dict) and ad_stats.get('error') == 429:
                        return pd.DataFrame(), "429_error"
                    
                    # Формирование отчета
                    metrics_df = await calculate_metrics_for_bot(orders, ad_stats, sheet_id)
                    summary = await generate_summary(metrics_df)

                    return metrics_df[[
                        'Артикул продавца',
                        'Кол-во заказов',
                        'Расходы РК',
                        'Прибыль'
                    ]], summary

        return pd.DataFrame(), ""
    except Exception as e:
        logging.error(f"Report generation error: {e}")
        return pd.DataFrame(), ""

if __name__ == "__main__":
    asyncio.run(main_from_config(CONFIG_URL))
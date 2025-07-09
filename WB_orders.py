import requests
import json
from datetime import datetime, timedelta
import time


def get_wb_grouped_stats(target_date, headers):
    """
    Получает статистику по всем карточкам товаров за указанную дату

    :param target_date: Дата в формате 'YYYY-MM-DD'
    :param headers: Заголовки запроса с авторизацией
    :return: Словарь со статистикой или None при ошибке
    """
    API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/grouped/history"

    # Формирование тела запроса
    payload = {
        "objectIDs": [],
        "brandNames": [],
        "tagIDs": [],
        "period": {
            "begin": target_date,
            "end": target_date
        },
        "timezone": "Europe/Moscow",
        "aggregationLevel": "day"
    }

    try:
        # Отправка запроса
        response = requests.post(
            API_URL,
            data=json.dumps(payload),
            headers=headers
        )

        # Проверка успешности запроса
        if response.status_code != 200:
            print(f"Ошибка API ({response.status_code}): {response.text}")
            return None

        data = response.json()

        # Проверка на ошибки в ответе
        if data.get("error"):
            print(
                f"Ошибка в ответе API: {data.get('errorText', 'Неизвестная ошибка')}")
            return None

        # Извлечение статистики
        stats = data.get("data", [])

        if not stats:
            print(f"Нет данных за {target_date}")
            return None

        # Получение истории из первой группы (все карточки)
        group_data = stats[0]
        daily_stats = group_data.get("history", [])

        if not daily_stats:
            print(f"Нет статистики за {target_date}")
            return None

        # Возвращаем статистику за запрошенный день
        return daily_stats[0]

    except requests.exceptions.RequestException as e:
        print(f"Ошибка соединения: {e}")
        return None
    except json.JSONDecodeError:
        print("Ошибка обработки JSON-ответа")
        return None


def get_wb_product_cards(headers):
    """
    Получает информацию по всем карточкам товаров с пагинацией

    :param api_key: API-ключ авторизации
    :return: Список словарей с данными по артикулам
    """
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"

    all_cards = []
    cursor = None
    request_count = 0
    start_time = time.time()

    try:
        while True:
            # Формируем тело запроса с пагинацией
            payload = {
                "settings": {
                    "filter": {"withPhoto": -1},
                    "limit": 100
                }
            }

            # Добавляем курсор для пагинации (кроме первого запроса)
            if cursor:
                payload["settings"]["cursor"] = {
                    "updatedAt": cursor["updatedAt"],
                    "nmID": cursor["nmID"]
                }

            # Отправляем запрос
            response = requests.post(url, json=payload, headers=headers)
            request_count += 1

            # Обработка ошибок
            if response.status_code != 200:
                print(f"Ошибка {response.status_code}: {response.text}")
                if response.status_code == 429:
                    reset_time = int(response.headers.get('Retry-After', 60))
                    print(f"Лимит запросов. Пауза {reset_time} сек.")
                    time.sleep(reset_time)
                    continue
                return None

            data = response.json()

            # Обработка каждой карточки
            for card in data.get("cards", []):
                all_cards.append({
                    "vendorCode": card.get("vendorCode"),
                    "nmID": card.get("nmID")
                })

            # Проверка завершения пагинации
            cursor = data.get("cursor")
            if not cursor or cursor.get("total", 0) <= 0:
                break

            # Контроль лимита запросов (100/мин)
            elapsed_time = time.time() - start_time
            if request_count >= 100 and elapsed_time < 60:
                sleep_time = 60 - elapsed_time + 1
                print(
                    f"Приближение к лимиту запросов. Пауза {sleep_time:.1f} сек.")
                time.sleep(sleep_time)
                request_count = 0
                start_time = time.time()

    except requests.exceptions.RequestException as e:
        print(f"Ошибка соединения: {e}")
        return None

    print(f"Получено карточек: {len(all_cards)}")
    return all_cards


def get_orders_statistics(headers, nm_ids, date_from=None, date_to=None):
    """
    Получает статистику по заказам для списка артикулов

    :param api_key: API-ключ авторизации
    :param nm_ids: Список nmID (артикулов WB)
    :param date_from: Начало периода в формате 'YYYY-MM-DD' (по умолчанию - 7 дней назад)
    :param date_to: Конец периода в формате 'YYYY-MM-DD' (по умолчанию - сегодня)
    :return: Словарь с статистикой в формате {nmID: {'ordersCount': X, 'ordersSumRub': Y}}
    """
    # Конфигурация запроса
    API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"

    # Рассчет дат по умолчанию
    if date_to is None:
        date_to = datetime.now().strftime("%Y-%m-%d")
    if date_from is None:
        date_from = (datetime.now()).strftime("%Y-%m-%d")

    # Разделяем артикулы на группы по 20 (ограничение API)
    chunks = [nm_ids[i:i + 20] for i in range(0, len(nm_ids), 20)]
    all_stats = {}
    # responses_count = 0
    try:
        for chunk in chunks:
            # Формирование тела запроса
            payload = {
                "nmIDs": chunk,
                "period": {
                    "begin": date_from,
                    "end": date_to
                },
                "timezone": "Europe/Moscow",
                "aggregationLevel": "day"
            }

            # Отправка запроса
            response = requests.post(
                API_URL,
                json=payload,
                headers=headers
            )

            # Обработка ответа
            if response.status_code == 200:
                data = response.json()

                if data.get("error"):
                    print(
                        f"Ошибка в ответе API: {data.get('errorText', 'Неизвестная ошибка')}")
                    continue

                # Обработка данных по каждому артикулу
                for item in data.get("data", []):
                    nm_id = item["nmID"]
                    orders_count = 0
                    orders_sum = 0.0
                    addToCartConversion = 0.0
                    cartToOrderConversion = 0.0

                    # Суммируем показатели за все дни периода
                    for day in item.get("history", []):
                        orders_count += day.get("ordersCount", 0)
                        orders_sum += day.get("ordersSumRub", 0)
                        addToCartConversion += day.get("addToCartConversion", 0)
                        cartToOrderConversion += day.get("cartToOrderConversion", 0)


                    all_stats[nm_id] = {
                        "ordersCount": orders_count,
                        "ordersSumRub": orders_sum,
                        "addToCartConversion": addToCartConversion,
                        "cartToOrderConversion": cartToOrderConversion
                    }

                # Добавляем артикулы без данных
                for nm_id in chunk:
                    if nm_id not in all_stats:
                        all_stats[nm_id] = {
                            "ordersCount": 0,
                            "ordersSumRub": 0.0,
                            "addToCartConversion": 0.0,
                            "cartToOrderConversion": 0.0
                        }

            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 20))
                print(f"Превышен лимит запросов. Пауза {retry_after} сек.")
                time.sleep(retry_after)
                # Повторяем запрос для текущего чанка
                chunks.append(chunk)
            else:
                print(f"Ошибка {response.status_code}: {response.text}")

            # # Соблюдаем лимит 3 запроса в минуту
            # responses_count += 1
            # if responses_count == 3:
            #     time.sleep(60)  # 60 сек / 3 запроса
            #     responses_count = 0

    except requests.exceptions.RequestException as e:
        print(f"Ошибка соединения: {e}")
    except json.JSONDecodeError:
        print("Ошибка обработки JSON-ответа")

    return all_stats


def get_dict_orders(headers, date):
    cards = get_wb_product_cards(headers)
    nm_ids = [product['nmID'] for product in cards]
    stats = get_orders_statistics(headers, nm_ids, date, date)

    # stats[nmId] = {ordersCount: ... , ordersSumRub: ...}
    return stats




# get_wb_grouped_stats('2025-07-04', )
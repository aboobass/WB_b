import aiohttp
import json
import time
import asyncio
import logging

async def get_wb_grouped_stats(target_date, headers):
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
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(
                API_URL,
                data=json.dumps(payload),
                timeout=30
            ) as response:
                # Проверка успешности запроса
                if response.status != 200:
                    print(f"Ошибка API ({response.status}): {await response.text()}")
                    return None

                data = await response.json()

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

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"Ошибка соединения: {e}")
        return None
    except json.JSONDecodeError:
        print("Ошибка обработки JSON-ответа")
        return None


async def get_wb_product_cards(headers):
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
    i = 0
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            while i < 50000:
                i+=1
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
                async with session.post(url, json=payload, timeout=30) as response:
                    request_count += 1

                    # Обработка ошибок
                    if response.status != 200:
                        print(f"Ошибка {response.status}: {await response.text()}")
                        if response.status == 429:
                            reset_time = 70
                            print(f"Лимит запросов. Пауза {reset_time} сек.")
                            await asyncio.sleep(reset_time)
                            continue
                        return None

                    data = await response.json()

                    # Обработка каждой карточки
                    for card in data.get("cards", []):
                        all_cards.append({
                            "vendorCode": card.get("vendorCode"),
                            "nmID": card.get("nmID")
                        })

                    # Проверка завершения пагинации
                    cursor = data.get("cursor")
                    if not cursor or cursor.get("total", 0) <= 100:
                        break

                    # Контроль лимита запросов (100/мин)
                    elapsed_time = time.time() - start_time
                    if request_count >= 100 and elapsed_time < 60:
                        sleep_time = 60 - elapsed_time + 1
                        print(
                            f"Приближение к лимиту запросов. Пауза {sleep_time:.1f} сек.")
                        await asyncio.sleep(sleep_time)
                        request_count = 0
                        start_time = time.time()

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"Ошибка соединения: {e}")
        return None

    print(f"Получено карточек: {len(all_cards)}")
    return all_cards

async def get_orders_statistics(headers, nm_ids, date_from=None, date_to=None, state=None):
    """Возвращает статистику с возможностью возобновления обработки"""
    API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"
    
    # Инициализация состояния
    if state is None:
        state = {
            'chunks': [nm_ids[i:i + 20] for i in range(0, len(nm_ids), 20)],
            'all_stats': {},
            'current_chunk': 0,
            'retry_count': 0
        }
    
    # Обрабатываем чанки по очереди
    while state['current_chunk'] < len(state['chunks']):
        chunk = state['chunks'][state['current_chunk']]
        payload = {
            "nmIDs": chunk,
            "period": {"begin": date_from, "end": date_to},
            "timezone": "Europe/Moscow",
            "aggregationLevel": "day"
        }

        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.post(
                    API_URL,
                    json=payload,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        for item in data.get("data", []):
                            nm_id = item["nmID"]
                            orders_count = 0
                            orders_sum = 0.0
                            addToCartConversion = 0.0
                            cartToOrderConversion = 0.0
                            for day in item.get("history", []):
                                orders_count += day.get("ordersCount", 0)
                                orders_sum += day.get("ordersSumRub", 0)
                                addToCartConversion += day.get("addToCartConversion", 0)
                                cartToOrderConversion += day.get("cartToOrderConversion", 0)
                            state['all_stats'][nm_id] = {
                                "ordersCount": orders_count,
                                "ordersSumRub": orders_sum,
                                "addToCartConversion": addToCartConversion,
                                "cartToOrderConversion": cartToOrderConversion
                            }
                        
                        # Переходим к следующему чанку
                        state['current_chunk'] += 1
                        state['retry_count'] = 0  # Сбрасываем счетчик повторов

                    elif response.status == 429:
                        retry_after = 20
                        logging.warning(f"429 error. Retry after: {retry_after}")
                        
                        # Увеличиваем счетчик повторов
                        state['retry_count'] += 1
                        if state['retry_count'] > 5:
                            logging.error("Max retries exceeded")
                            return {
                                'error': 429,
                                'retry_after': retry_after,
                                'state': state
                            }
                        
                        # Возвращаем текущее состояние для возобновления
                        return {
                            'error': 429,
                            'retry_after': retry_after,
                            'state': state
                        }

                    else:
                        logging.error(f"Error {response.status}: {await response.text()}")
                        # Переходим к следующему чанку при других ошибках
                        state['current_chunk'] += 1
                        state['retry_count'] = 0

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.error(f"Request error: {e}")
            state['current_chunk'] += 1
            state['retry_count'] = 0

    return state['all_stats']

async def get_dict_orders(headers, date, state=None, cards=None):
    """Возвращает статистику по заказам с возможностью возобновления"""
    if not cards:
        cards = await get_wb_product_cards(headers)
    if not cards:
        return {}
    
    nm_ids = [product['nmID'] for product in cards]
    return await get_orders_statistics(headers, nm_ids, date, date, state)
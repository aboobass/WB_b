from datetime import datetime
import requests
from time import sleep
import json
from datetime import datetime, timedelta
from collections import defaultdict

API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2NjIxMzM0MiwiaWQiOiIwMTk3OGVhYy0zZWVhLTc5MTUtOGY5OS02ZjA5MmVjZmQyMWIiLCJpaWQiOjkwNTkxNzg2LCJvaWQiOjEwNDQxNTAsInMiOjE2MTI2LCJzaWQiOiJhODA2NWM4Mi0wODg3LTQxZjktODlmZS02OWRlZjVmYTAzM2YiLCJ0IjpmYWxzZSwidWlkIjo5MDU5MTc4Nn0.2tY1tLfenFOOJU_IO0C_JVMe_tucFvWekVCV5Z0IskfK9ESMo7yCDZYs7fH7HRA6Hv0-Ls_9bzv4d_Rx2lQ6fQ"
HEADERS = {"Authorization": API_KEY}

def safe_request(HEADERS, url, method='GET', json_data=None, params=None, max_retries=3):
    """Безопасный запрос с обработкой ошибок и повторами"""
    for attempt in range(max_retries):
        try:
            if method == 'GET':
                response = requests.get(
                    url, headers=HEADERS, params=params, json=json_data)
            elif method == 'POST':
                response = requests.post(
                    url, headers=HEADERS, json=json_data, params=params)

            if response.status_code == 400 and "no companies with correct intervals" in response.text:
                return None  # Молча завершить, не печатая и не повторяя
            
            # Обработка 429 Too Many Requests
            if response.status_code == 429:
                return "429_error"
              
            # Обработка 204 No Content
            if response.status_code == 204:
                return None

            # Проверка на успешный статус
            if 200 <= response.status_code < 300:
                try:
                    return response.json()
                except json.JSONDecodeError:
                    return None
            else:
                print(
                    f"Ошибка сервера ({response.status_code}): {response.text[:500]}")

        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса ({attempt+1}/{max_retries}): {e}")
            sleep(2)

    print(f"Не удалось выполнить запрос к {url}")
    return None

def get_promotion_campaigns(HEADERS):
    """Получение рекламных компаний с детализацией"""
    count_url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
    count_data = safe_request(HEADERS, count_url, 'GET')
    if not count_data:
        return []

    advert_ids = [advert['advertId']
                  for group in count_data.get('adverts', [])
                  for advert in group.get('advert_list', [])]

    if not advert_ids:
        return []

    result = []
    chunk_size = 50
    for i in range(0, len(advert_ids), chunk_size):
        chunk = advert_ids[i:i+chunk_size]

        adverts_url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
        campaigns = safe_request(HEADERS, adverts_url, 'POST', json_data=chunk)

        if not campaigns:
            continue

        for campaign in campaigns:
            nm_ids = []
            type_comp = 'no'
            if 'params' in campaign:
                for param in campaign['params']:
                    for nm_item in param.get('nms', []):
                        if isinstance(nm_item, dict):
                            nm_ids.append(nm_item.get('nm'))
                        else:
                            nm_ids.append(nm_item)

            if 'autoParams' in campaign:
                auto_nms = campaign['autoParams'].get('nms', [])
                if isinstance(auto_nms, list):
                    nm_ids.extend(auto_nms)
                type_comp = 'auto'
            if 'unitedParams' in campaign:
                for param in campaign['unitedParams']:
                    united_nms = param.get('nms', [])
                    if isinstance(united_nms, list):
                        nm_ids.extend(united_nms)
                type_comp = 'auction'

            nm_ids = list(set(filter(lambda x: x is not None, nm_ids)))

            result.append({
                'advertId': campaign['advertId'],
                'type': 'promotion',
                'tipe_comp': type_comp,
                'createTime': campaign.get('createTime', ''),
                'expenses': 0,  # Временное значение
                'nmIds': nm_ids
            })
    return result

def get_expenses_per_nm(HEADERS, date=None):
    """Возвращает словарь с затратами на каждый артикул {nmId: сумма}"""
    # Получаем список кампаний с nmIds
    campaigns = get_promotion_campaigns(HEADERS)
    if not campaigns:
        return {}

    # Текущая дата
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    else:
        date = date[:10]

    # Формируем запросы по 100 кампаний
    nm_expenses = defaultdict(dict )
    auto_ctr = 0.0
    auction_ctr = 0.0
            
    for i in range(0, len(campaigns), 100):
        chunk = campaigns[i:i+100]
        request_body = []

        for campaign in chunk:
            request_body.append({
                "id": campaign['advertId'],
                'dates': [date]
            })

        # Отправляем запрос для группы кампаний
        fullstats_url = "https://advert-api.wildberries.ru/adv/v2/fullstats"
        response = safe_request(HEADERS, fullstats_url,
                                'POST', json_data=request_body)

        if not response or not isinstance(response, list):
            continue

        # Обрабатываем ответ для каждой кампании в группе
        for campaign_data in response:
            advert_id = campaign_data.get('advertId')
            total_expense = campaign_data.get('sum', 0)

            views = campaign_data.get('views', 0)

            # Находим соответствующую кампанию в нашем списке
            campaign = next(
                (c for c in chunk if c['advertId'] == advert_id), None)
            if not campaign:
                continue

            nmIds = campaign['nmIds']
            if not nmIds or total_expense <= 0:
                continue
            
            if campaign['tipe_comp'] == 'auto':
                auto_ctr = float(campaign_data.get('ctr', 0))
            elif campaign['tipe_comp'] == 'auction':
                auction_ctr = float(campaign_data.get('ctr', 0))
            else:
                auto_ctr = 0
                auction_ctr = 0

            # Распределяем затраты по артикулам
            expense_per_nm = total_expense / len(nmIds)
            views_per_nm = views / len(nmIds)
            auto_ctr_per_nm = auto_ctr / len(nmIds)
            auction_ctr_per_nm =  auction_ctr / len(nmIds)
            for nmId in nmIds:
                nm_expenses[nmId]['sum'] = nm_expenses[nmId].get('sum', 0) + expense_per_nm
                nm_expenses[nmId]['views'] = nm_expenses[nmId].get('views', 0) + views_per_nm
                nm_expenses[nmId]['auto_ctr'] = nm_expenses[nmId].get('auto_ctr', 0) + auto_ctr_per_nm
                nm_expenses[nmId]['auction_ctr'] = nm_expenses[nmId].get('auction_ctr', 0) + auction_ctr_per_nm
            # print(nm_expenses)
    return dict(nm_expenses)
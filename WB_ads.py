from datetime import datetime
import requests
import asyncio
import json
from datetime import datetime, timedelta
from collections import defaultdict
import logging

async def safe_request(HEADERS, url, method='GET', json_data=None, params=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            if method == 'GET':
                response = requests.get(
                    url, headers=HEADERS, params=params, json=json_data, timeout=30)
            elif method == 'POST':
                response = requests.post(
                    url, headers=HEADERS, json=json_data, params=params, timeout=30)

            # Обработка 429
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 20))
                logging.warning(f"429 error. Retry after: {retry_after}")
                return {
                    'error': 429,
                    'retry_after': retry_after
                }
                
            # Остальная обработка как раньше...
            if response.status_code == 400 and "no companies with correct intervals" in response.text:
                return None
            if response.status_code == 204:
                return None
            if 200 <= response.status_code < 300:
                try:
                    return response.json()
                except json.JSONDecodeError:
                    return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Request error ({attempt+1}/{max_retries}): {e}")
            await asyncio.sleep(2)
    
    logging.error(f"Failed to request: {url}")
    return None


async def get_promotion_campaigns(HEADERS):
    """Получение рекламных компаний с детализацией"""
    count_url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
    count_data = await safe_request(HEADERS, count_url, 'GET')
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
        campaigns = await safe_request(HEADERS, adverts_url, 'POST', json_data=chunk)

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

async def get_expenses_per_nm(HEADERS, date=None):
    """Возвращает расходы с возможностью возобновления обработки"""
    # Получаем список кампаний с nmIds
    campaigns = await get_promotion_campaigns(HEADERS)
    if not campaigns:
        return {}

    # Текущая дата
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    else:
        date = date[:10]

    # Формируем запросы по 100 кампаний
    nm_expenses = defaultdict(dict)
    auto_ctr = 0.0
    auction_ctr = 0.0
    
    i = 0
    while i < len(campaigns):
        chunk = campaigns[i:i+100]
        request_body = []

        for campaign in chunk:
            request_body.append({
                "id": campaign['advertId'],
                'dates': [date]
            })

        # Отправляем запрос для группы кампаний
        fullstats_url = "https://advert-api.wildberries.ru/adv/v2/fullstats"
        response = await safe_request(HEADERS, fullstats_url,
                                'POST', json_data=request_body)

        # Обработка 429 ошибки
        if isinstance(response, dict) and response.get('error') == 429:
            retry_after = response.get('retry_after', 20)
            logging.warning(f"Ads API 429 error. Retry after: {retry_after}")
            await asyncio.sleep(retry_after)
            continue  # Повторяем с теми же данными
            
        if not response or not isinstance(response, list):
            i += len(chunk)
            continue

        # Обрабатываем ответ
        for campaign_data in response:
            advert_id = campaign_data.get('advertId')

            # Находим соответствующую кампанию в нашем списке
            campaign = next(
                (c for c in chunk if c['advertId'] == advert_id), None)
            if not campaign:
                continue

            nmIds = campaign['nmIds']
            if not nmIds: #or total_expense <= 0:
                continue
            
            views_per_nm = {}
            total_expense_per_nm = {}

            auto_views_per_nm = {}
            auto_clicks_per_nm = {}
            
            auction_views_per_nm = {}
            auction_clicks_per_nm = {}
            # print(campaign_data['days'][0])
            for camp_apps in campaign_data['days'][0]['apps']:
                for camp_nms in camp_apps['nm']:
                    nm_Id = camp_nms['nmId']
                    total_expense_per_nm[nm_Id] =  total_expense_per_nm.get(nm_Id, 0) + camp_nms.get('sum', 0)
                    views_per_nm[nm_Id] = views_per_nm.get(nm_Id, 0) + camp_nms.get('views', 0)
                    if campaign['tipe_comp'] == 'auto':
                        auto_clicks_per_nm[nm_Id] = auto_clicks_per_nm.get(nm_Id, 0) + camp_nms.get('clicks', 0)
                        auto_views_per_nm[nm_Id] = auto_views_per_nm.get(nm_Id, 0) + camp_nms.get('views', 0)

                    elif campaign['tipe_comp'] == 'auction':
                        auction_clicks_per_nm[nm_Id] = auction_clicks_per_nm.get(nm_Id, 0) + camp_nms.get('clicks', 0)
                        auction_views_per_nm[nm_Id] = auction_views_per_nm.get(nm_Id, 0) + camp_nms.get('views', 0)


                        

            # Распределяем затраты по артикулам
            # expense_per_nm = total_expense / len(nmIds)
            # views_per_nm = views / len(nmIds)
            # auto_ctr_per_nm = auto_ctr / len(nmIds)
            # auction_ctr_per_nm =  auction_ctr / len(nmIds)
            for nmId in nmIds:
                nm_expenses[nmId]['sum'] = nm_expenses[nmId].get('sum', 0) + total_expense_per_nm.get(nmId, 0)
                nm_expenses[nmId]['views'] = nm_expenses[nmId].get('views', 0) + views_per_nm.get(nmId, 0)

                nm_expenses[nmId]['auto_clicks'] = nm_expenses[nmId].get('auto_clicks', 0) + auto_clicks_per_nm.get(nmId, 0)
                nm_expenses[nmId]['auto_views'] = nm_expenses[nmId].get('auto_views', 0) + auto_views_per_nm.get(nmId, 0)
                nm_expenses[nmId]['auction_clicks'] = nm_expenses[nmId].get('auction_clicks', 0) + auction_clicks_per_nm.get(nmId, 0)
                nm_expenses[nmId]['auction_views'] = nm_expenses[nmId].get('auction_views', 0) + auction_views_per_nm.get(nmId, 0)
            
            # print(nm_expenses)
        i += len(chunk)
    
    return dict(nm_expenses)
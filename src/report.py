import json
import locale
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from enum import Enum
from typing import ContextManager, Dict, List, Union

import requests

from src.notification import send_message

logger = logging.getLogger(__name__)


@contextmanager
def locale_manager(locale_name: str) -> ContextManager[None]:
    try:
        locale.setlocale(category=locale.LC_ALL, locale=locale_name)
    except locale.Error:
        locale.setlocale(category=locale.LC_ALL, locale='ru')
    try:
        yield
    finally:
        locale.setlocale(category=locale.LC_ALL, locale='')


class DateRange(Enum):
    LAST_1_DAY = 1
    LAST_3_DAYS = 3
    LAST_7_DAYS = 7
    LAST_14_DAYS = 14
    LAST_30_DAYS = 30
    LAST_90_DAYS = 90
    LAST_180_DAYS = 180
    LAST_366_DAYS = 366

    def get_date_range(self, return_type: str = 'str') -> Union[str, tuple]:
        if return_type not in ['str', 'datetime']:
            raise ValueError('return_type should be str or datetime')

        today = datetime.now().date()
        start_date = today - timedelta(days=self.value)

        if return_type == 'str':
            return start_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
        else:
            return start_date, today


class MyTargetAPI:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = 'https://target.my.com'

    @contextmanager
    def session(self) -> ContextManager[requests.Session]:
        session = requests.Session()
        session.headers.update({'Authorization': f'Bearer {self.access_token}'})
        try:
            yield session
        finally:
            session.close()

    def get_campaigns(self, session: requests.Session) -> List[Dict[str, Union[int, str]]]:
        url = f'{self.base_url}/api/v2/campaigns.json'
        response = session.get(url)
        return [{'campaign_id': item['id'], 'campaign_name': item['name']} for item in response.json()['items']]

    def get_banners(self, session: requests.Session, campaign_id: int) -> List[Dict[str, Union[int, str]]]:
        url = f'{self.base_url}/api/v2/banners.json'
        params = {'_campaign_id': campaign_id}
        response = session.get(url, params=params)
        return [{'ad_group_id': item['ad_group_id'], 'banner_id': item['id']} for item in response.json()['items']]

    def get_statistics(self, session: requests.Session, banner_id: int, date_range: DateRange) -> Dict:
        retry_count = 0
        while retry_count < 3:
            start_date, end_date = date_range.get_date_range()

            url = f'{self.base_url}/api/v2/statistics/banners/day.json'
            params = {
                'id': banner_id,
                'date_from': start_date,
                'date_to': end_date,
                'metrics': 'all'
            }
            response = session.get(url, params=params)

            statistics = response.json()
            if 'items' in statistics:
                return statistics

        if retry_count == 3:
            logger.error(f'Failed to get statistics for banner_id: {banner_id}')
            raise requests.RequestException('Failed to get statistics for banner_id: {banner_id}')


def download_statistics(access_token: str, statistics_file: str, date_range: DateRange) -> None:
    logger.info(f'Starting statistics download...')

    log_messages = []

    api = MyTargetAPI(access_token)
    with api.session() as session:
        campaigns = api.get_campaigns(session)
        banners_group = [api.get_banners(session, campaign['campaign_id']) for campaign in campaigns]

        flattened_data = [{
            'campaign_id': campaign['campaign_id'],
            'campaign_name': campaign['campaign_name'],
            'ad_group_id': banner['ad_group_id'],
            'banner_id': banner['banner_id']
        } for campaign in campaigns for banners in banners_group for banner in banners]

        for item in flattened_data:
            campaign_id = item['campaign_id']
            ad_group_id = item['ad_group_id']
            banner_id = item['banner_id']

            formatted_statistics_file = statistics_file.\
                format(campaign_id=campaign_id, ad_group_id=ad_group_id, banner_id=banner_id)

            if os.path.exists(formatted_statistics_file):
                log_messages.append(f'{campaign_id}_{ad_group_id}_{banner_id}: statistics already exists...')
                continue

            data = api.get_statistics(session, item['banner_id'], date_range)

            rows = data['items'][0]['rows']

            statistics = [{
                'campaign_id': item['campaign_id'],
                'campaign_name': item['campaign_name'],
                'ad_group_id': item['ad_group_id'],
                'banner_id': item['banner_id'],
                **{f'{k1}_{k2}' if isinstance(v1, dict) else k1: v2 for k1, v1 in row.items() for k2, v2 in
                   (v1.items() if isinstance(v1, dict) else [(k1, v1)])}
            } for row in rows]

            with open(formatted_statistics_file, 'w', encoding='utf-8') as file:
                json.dump(statistics, file, ensure_ascii=False, indent=2)

            log_messages.append(f'{campaign_id}_{ad_group_id}_{banner_id}: statistics saved...')
            logger.info(f'Report {formatted_statistics_file} saved...')

    send_message('\n'.join(log_messages))
    logger.info(f'All statistics download finished...')

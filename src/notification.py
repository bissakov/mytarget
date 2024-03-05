import logging
import os
from urllib.parse import urljoin

import dotenv
import requests
from requests.adapters import HTTPAdapter


class TelegramBot:
    def __init__(self, token: str, chat_id: str) -> None:
        self.api_url = f'https://api.telegram.org/bot{token}/'
        self.chat_id = chat_id
        self.retry_count = 10
        self.session = requests.Session()
        self.session.mount('http://', HTTPAdapter(max_retries=5))

    def send_request(self, url, data, files=None):
        for attempt in range(self.retry_count):
            try:
                response = self.session.post(url, data=data, files=files)
                if response.status_code == 200:
                    return True
                logging.warning(f'Retry {attempt + 1}: Sending failed with status code {response.status_code}')
            except requests.RequestException as e:
                logging.error(f'Request failed: {e}')
                self.session = requests.Session()
        try:
            response = requests.post(url, data=data, files=files)
            logging.info(f'Final attempt using direct requests.post: {response}')
            return response.status_code == 200
        except requests.RequestException as e:
            logging.error(f'Final attempt failed: {e}')
            return False

    def send_message(self, message: str) -> bool:
        send_data = {'chat_id': self.chat_id, 'text': message}
        url = urljoin(self.api_url, 'sendMessage')
        return self.send_request(url, send_data)

    def send_document(self, file_path: str, caption: str = '') -> bool:
        send_data = {'chat_id': self.chat_id}
        if caption:
            send_data['caption'] = caption
        url = urljoin(self.api_url, 'sendDocument')
        with open(file_path, 'rb') as document:
            files = {'document': document}
            return self.send_request(url, send_data, files=files)


if 'TG_TOKEN' not in os.environ:
    dotenv.load_dotenv()

TG_TOKEN = os.getenv('TG_TOKEN')
TG_CHAT_ID = os.getenv('TG_CHAT_ID')
BOT = TelegramBot(token=TG_TOKEN, chat_id=TG_CHAT_ID)


def send_message(message: str) -> None:
    BOT.send_message(message=message)


def send_document(file_path: str, caption: str = '') -> None:
    BOT.send_document(file_path=file_path, caption=caption)

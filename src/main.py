import logging
import os
import sys
import traceback
from datetime import datetime
from os.path import abspath, dirname, join

import dotenv
import pytz

if sys.version_info < (3,):
    raise Exception('Python 2 is not supported')

project_root = dirname(dirname(abspath(__file__)))
sys.path.append(project_root)
dotenv.load_dotenv()


if __name__ == '__main__':
    from src.logger import setup_logger
    from src.notification import send_document, send_message
    from src.report import DateRange, download_statistics, locale_manager
    from src.db import populate_db

    today = datetime.now(pytz.timezone('Asia/Almaty'))
    logger_file = setup_logger(today=today, project_root=project_root)
    logger = logging.getLogger(__name__)

    if os.getenv('PROD') == '1':
        logger.info('Production mode')
        connection_url = (
            'mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver}'
            .format(
                user=os.getenv('USER'),
                password=os.getenv('PASSWORD'),
                host=os.getenv('HOST'),
                port=os.getenv('PORT'),
                database='{database}',
                driver=os.getenv('DRIVER')
            )
        )
    else:
        logger.info('Development mode')
        connection_url = f'sqlite:///{join(project_root, "mytarget.db")}'

    database = os.getenv('DATABASE')
    today_date = today.date()


    def main():
        try:
            send_message('### MYTARGET ###')
            send_message(f'Starting process for {today_date}')
            logger.info(f'Starting process for {today_date}')

            access_tokens = os.getenv('ACCESS_TOKENS').split(',')
            year_month = today.strftime('%Y/%B')

            with locale_manager('en_US.UTF-8'):
                folder = join(project_root, 'statistics', year_month, f'{today_date}')
            os.makedirs(folder, exist_ok=True)

            statistics_file = join(folder, '{campaign_id}_{ad_group_id}_{banner_id}.json')
            for i, access_token in enumerate(access_tokens):
                download_statistics(access_token, statistics_file, DateRange.LAST_3_DAYS)

            log_messages = []
            for file in os.listdir(folder):
                file = join(folder, file)
                message = populate_db(file, connection_url, database)
                log_messages.append(message)
            send_message('\n'.join(log_messages))

        except Exception as exc:
            exc_message = traceback.format_exc()
            send_document(logger_file, caption=exc_message)
            logger.exception(exc)
            raise exc

        send_document(logger_file)
        send_message(f'Successfully finished for {today_date}')
        logger.info(f'Successfully finished for {today_date}')

    main()

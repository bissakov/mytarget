import logging
import os
from datetime import datetime
from os.path import join


def setup_logger(today: datetime, project_root: str) -> str:
    logging.Formatter.converter = lambda *args: today.timetuple()

    log_folder = join(project_root, 'logs')
    os.makedirs(log_folder, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    log_format = '%(asctime).19s %(levelname)s %(name)s %(filename)s %(funcName)s : %(message)s'
    formatter = logging.Formatter(log_format)

    today_str = today.strftime('%d.%m.%y')
    year_month_folder = join(log_folder, today.strftime('%Y/%B'))
    os.makedirs(year_month_folder, exist_ok=True)

    logger_file = join(year_month_folder, f'{today_str}.log')
    file_handler = logging.FileHandler(
        logger_file,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    httpcore_logger = logging.getLogger('httpcore')
    httpcore_logger.setLevel(logging.INFO)

    logger.addHandler(file_handler)

    return logger_file

import logging
from os.path import basename
from typing import Generator

import pandas as pd
from sqlalchemy import Table, create_engine
from sqlalchemy.orm import sessionmaker

from src.models import AdGroup, Banner, Base, Campaign, Performance
from src.notification import send_message

logger = logging.getLogger(__name__)


class Dataset:
    def __init__(self, statistics_file: str):
        self.statistics_df = pd.read_json(statistics_file, orient='records')
        self.size = len(self.statistics_df)

    def is_empty(self) -> bool:
        return self.size == 0

    @staticmethod
    def columns(table: Table) -> list[str]:
        return [col for col in table.columns.keys()]

    def campaigns(self) -> Generator[Campaign, None, None]:
        for _, campaign in self.statistics_df[self.columns(Campaign.__table__)].drop_duplicates().iterrows():
            yield Campaign(**campaign.to_dict())

    def ad_groups(self) -> Generator[AdGroup, None, None]:
        for _, ad_group in self.statistics_df[self.columns(AdGroup.__table__)].drop_duplicates().iterrows():
            yield AdGroup(**ad_group.to_dict())

    def banners(self) -> Generator[Banner, None, None]:
        for _, ad in self.statistics_df[self.columns(Banner.__table__)].drop_duplicates().iterrows():
            yield Banner(**ad.to_dict())

    def performance(self) -> Generator[Performance, None, None]:
        performance_columns = [col for col in self.columns(Performance.__table__) if col != 'performance_id']
        for _, performance in self.statistics_df[performance_columns].drop_duplicates().iterrows():
            yield Performance(**performance.to_dict())


def populate_db(statistics_file: str, connection_url: str, database: str) -> str:
    logger.info(f'Starting DB population...')

    if 'sqlite' in connection_url:
        engine = create_engine(connection_url, echo=False)
    else:
        connection_url = connection_url.format(database=database)
        engine = create_engine(connection_url, use_setinputsizes=False, echo=False)

    file_name_no_ext = basename(statistics_file).split('.')[0]

    Base.metadata.create_all(engine)

    dataset = Dataset(statistics_file)
    dataset_size = dataset.size

    if dataset.is_empty():
        logger.info(f'{file_name_no_ext}: JSON statistics are empty.')
        return f'{file_name_no_ext}: JSON statistics are empty.'

    with sessionmaker(engine, autocommit=False, autoflush=False)() as session:
        for campaign in dataset.campaigns():
            session.merge(campaign)

        for ad_group in dataset.ad_groups():
            session.merge(ad_group)

        for ad in dataset.banners():
            session.merge(ad)

        for idx, performance in enumerate(dataset.performance(), start=1):
            if idx % 10000 == 0 or idx == dataset_size:
                logger.info(f'Processed {idx}/{dataset_size} rows.')
                session.commit()
            session.merge(performance)

        session.commit()

    logger.info(f'{file_name_no_ext}: DB populated {dataset_size} rows.')
    return f'{file_name_no_ext}: DB populated {dataset_size} rows.'

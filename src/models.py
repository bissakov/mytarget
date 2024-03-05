import hashlib
import logging
import os

from sqlalchemy import BigInteger, Column, Date, Float, ForeignKey, Integer, MetaData, NVARCHAR
from sqlalchemy.orm import declarative_base, relationship


schema = 'dbo' if bool(int(os.getenv('PROD'), 0)) else None
logger = logging.getLogger(__name__)
metadata = MetaData(schema=schema)
Base = declarative_base(metadata=metadata)


class Campaign(Base):
    __tablename__ = 'Campaigns'
    campaign_id = Column(BigInteger, primary_key=True, index=True)
    campaign_name = Column(NVARCHAR(100))
    ad_groups = relationship('AdGroup', back_populates='campaign')

    def __eq__(self, other):
        return self.campaign_id == other.campaign_id

    def __hash__(self):
        return hash(self.campaign_id)

    def __repr__(self):
        return f'<Campaign {self.campaign_id}>'


class AdGroup(Base):
    __tablename__ = 'AdGroups'
    ad_group_id = Column(BigInteger, primary_key=True, index=True)
    campaign_id = Column(BigInteger, ForeignKey('Campaigns.campaign_id'), index=True)
    campaign = relationship('Campaign', back_populates='ad_groups')
    banners = relationship('Banner', back_populates='ad_group')

    def __eq__(self, other):
        return self.ad_group_id == other.ad_group_id and self.campaign_id == other.campaign_id

    def __hash__(self):
        return hash(self.ad_group_id) ^ hash(self.campaign_id)

    def __repr__(self):
        return f'<AdGroup {self.ad_group_id}>'


class Banner(Base):
    __tablename__ = 'Banners'
    banner_id = Column(BigInteger, primary_key=True)
    ad_group_id = Column(BigInteger, ForeignKey('AdGroups.ad_group_id'), index=True)
    campaign_id = Column(BigInteger, ForeignKey('Campaigns.campaign_id'), index=True)
    ad_group = relationship('AdGroup', back_populates='banners')
    performances = relationship('Performance', back_populates='banner')

    def __eq__(self, other):
        return self.banner_id == other.banner_id and self.ad_group_id == other.ad_group_id and self.campaign_id == other.campaign_id

    def __hash__(self):
        return hash(self.banner_id) ^ hash(self.ad_group_id) ^ hash(self.campaign_id)

    def __repr__(self):
        return f'<Banner {self.banner_id}>'


class Performance(Base):
    __tablename__ = 'Performance'
    performance_id = Column(NVARCHAR(32), primary_key=True, index=True)
    campaign_id = Column(BigInteger, ForeignKey('Campaigns.campaign_id'), index=True)
    ad_group_id = Column(BigInteger, ForeignKey('AdGroups.ad_group_id'), index=True)
    banner_id = Column(BigInteger, ForeignKey('Banners.banner_id'), index=True)
    date = Column(Date, index=True)
    base_shows = Column(Integer)
    base_clicks = Column(Integer)
    base_goals = Column(Float)
    base_spent = Column(Float)
    base_cpm = Column(Float)
    base_cpc = Column(Float)
    base_cpa = Column(Float)
    base_ctr = Column(Float)
    base_cr = Column(Float)
    events_opening_app = Column(Float)
    events_opening_post = Column(Float)
    events_moving_into_group = Column(Float)
    events_clicks_on_external_url = Column(Float)
    events_launching_video = Column(Float)
    events_comments = Column(Float)
    events_joinings = Column(Integer)
    events_likes = Column(Float)
    events_shares = Column(Float)
    events_votings = Column(Float)
    events_sending_form = Column(Float)
    uniques_reach = Column(Integer)
    uniques_total = Column(Integer)
    uniques_increment = Column(Integer)
    uniques_frequency = Column(Float)
    uniques_video_started = Column(Integer)
    uniques_video_viewed_10_seconds = Column(Integer)
    uniques_video_viewed_25_percent = Column(Integer)
    uniques_video_viewed_50_percent = Column(Integer)
    uniques_video_viewed_75_percent = Column(Integer)
    uniques_video_viewed_100_percent = Column(Integer)
    uniques_video_viewed_10_seconds_rate = Column(Integer)
    uniques_video_viewed_25_percent_rate = Column(Integer)
    uniques_video_viewed_50_percent_rate = Column(Integer)
    uniques_video_viewed_75_percent_rate = Column(Integer)
    uniques_video_viewed_100_percent_rate = Column(Integer)
    uniques_video_viewed_range_rate = Column(NVARCHAR(12))
    uniques_video_depth_of_view = Column(Integer)
    video_started = Column(Integer)
    video_paused = Column(Integer)
    video_resumed_after_pause = Column(Integer)
    video_fullscreen_on = Column(Integer)
    video_fullscreen_off = Column(Integer)
    video_sound_turned_off = Column(Integer)
    video_sound_turned_on = Column(Integer)
    video_viewed_10_seconds = Column(Integer)
    video_viewed_25_percent = Column(Integer)
    video_viewed_50_percent = Column(Integer)
    video_viewed_75_percent = Column(Integer)
    video_viewed_100_percent = Column(Integer)
    video_viewed_10_seconds_rate = Column(Integer)
    video_viewed_25_percent_rate = Column(Integer)
    video_viewed_50_percent_rate = Column(Integer)
    video_viewed_75_percent_rate = Column(Integer)
    video_viewed_100_percent_rate = Column(Integer)
    video_viewed_range_rate = Column(NVARCHAR(12))
    video_depth_of_view = Column(Integer)
    video_started_cost = Column(Float)
    video_viewed_10_seconds_cost = Column(Float)
    video_viewed_25_percent_cost = Column(Float)
    video_viewed_50_percent_cost = Column(Float)
    video_viewed_75_percent_cost = Column(Float)
    video_viewed_100_percent_cost = Column(Float)
    carousel_slide_1_clicks = Column(Float)
    carousel_slide_1_shows = Column(Float)
    carousel_slide_2_clicks = Column(Float)
    carousel_slide_2_shows = Column(Float)
    carousel_slide_3_clicks = Column(Float)
    carousel_slide_3_shows = Column(Float)
    carousel_slide_4_clicks = Column(Float)
    carousel_slide_4_shows = Column(Float)
    carousel_slide_5_clicks = Column(Float)
    carousel_slide_5_shows = Column(Float)
    carousel_slide_6_clicks = Column(Float)
    carousel_slide_6_shows = Column(Float)
    carousel_slide_1_ctr = Column(Float)
    carousel_slide_2_ctr = Column(Float)
    carousel_slide_3_ctr = Column(Float)
    carousel_slide_4_ctr = Column(Float)
    carousel_slide_5_ctr = Column(Float)
    carousel_slide_6_ctr = Column(Float)
    ad_offers_offer_postponed = Column(Float)
    ad_offers_upload_receipt = Column(Float)
    ad_offers_earn_offer_rewards = Column(Float)
    playable_playable_game_open = Column(Float)
    playable_playable_game_close = Column(Float)
    playable_playable_call_to_action = Column(Float)
    tps_tps = Column(Float)
    tps_tpd = Column(Float)
    moat_impressions = Column(Integer)
    moat_in_view = Column(Integer)
    moat_never_focused = Column(Integer)
    moat_never_visible = Column(Integer)
    moat_never_50_perc_visible = Column(Integer)
    moat_never_1_sec_visible = Column(Integer)
    moat_human_impressions = Column(Integer)
    moat_impressions_analyzed = Column(Integer)
    moat_in_view_percent = Column(Integer)
    moat_human_and_viewable_perc = Column(Integer)
    moat_never_focused_percent = Column(Integer)
    moat_never_visible_percent = Column(Integer)
    moat_never_50_perc_visible_percent = Column(Integer)
    moat_never_1_sec_visible_percent = Column(Integer)
    moat_in_view_diff_percent = Column(Integer)
    moat_active_in_view_time = Column(Integer)
    moat_attention_quality = Column(Integer)
    social_network_vk_join = Column(Integer)
    social_network_ok_join = Column(Integer)
    social_network_dzen_join = Column(Integer)
    social_network_result_join = Column(Integer)
    social_network_vk_message = Column(Integer)
    social_network_ok_message = Column(Integer)
    social_network_result_message = Column(Integer)
    romi_value = Column(Float)
    romi_romi = Column(Float)
    romi_adv_cost_share = Column(Float)
    banner = relationship('Banner', back_populates='performances')

    def __init__(self, campaign_id: int, ad_group_id: int, banner_id: int, date: date, **kwargs) -> None:
        self.campaign_id = campaign_id
        self.ad_group_id = ad_group_id
        self.banner_id = banner_id
        self.date = date
        self.performance_id = self.generate_performance_id()
        super().__init__(**kwargs)

    def generate_performance_id(self) -> str:
        combined_string = f'{self.campaign_id}_{self.ad_group_id}_{self.banner_id}_{self.date.isoformat()}'
        hash_object = hashlib.md5(combined_string.encode())
        return hash_object.hexdigest()

    def __eq__(self, other):
        return self.performance_id == other.performance_id

    def __hash__(self):
        return hash(self.performance_id)

    def __repr__(self):
        return f'<Performance {self.performance_id}>'

from backend.db import Base
from sqlalchemy import Column, String, Integer, Float, ForeignKey

class PlayerWeeklyStats(Base):
    __tablename__ = 'PlayerWeeklyStats'

    player_id = Column(String(50), primary_key=True, nullable=False)
    season = Column(Integer, primary_key=True, nullable=False)
    season_type = Column(String(20), primary_key=True, nullable=False)
    week = Column(Integer, primary_key=True, nullable=False)

    team_id = Column(String(10), ForeignKey('DimTeams.team_id'), nullable=False)

    shotgun = Column(Integer)
    no_huddle = Column(Integer)
    qb_dropback = Column(Integer)
    qb_scramble = Column(Integer)
    pass_attempts = Column(Integer)
    complete_pass = Column(Integer)
    incomplete_pass = Column(Integer)
    passing_yards = Column(Float)
    receiving_yards = Column(Float)
    yards_after_catch = Column(Float)
    rush_attempts = Column(Integer)
    rushing_yards = Column(Float)
    tackled_for_loss = Column(Integer)
    first_down_pass = Column(Integer)
    first_down_rush = Column(Integer)
    third_down_converted = Column(Integer)
    third_down_failed = Column(Integer)
    fourth_down_converted = Column(Integer)
    fourth_down_failed = Column(Integer)
    rush_touchdown = Column(Integer)
    pass_touchdown = Column(Integer)
    receiving_touchdown = Column(Integer)
    receptions = Column(Integer)
    targets = Column(Integer)
    passing_air_yards = Column(Float)
    receiving_air_yards = Column(Float)
    fantasy_points_ppr = Column(Float)
    fantasy_points_standard = Column(Float)
    passer_rating = Column(Float)
    adot = Column(Float)
    air_yards_share = Column(Float)
    target_share = Column(Float)
    comp_pct = Column(Float)
    int_pct = Column(Float)
    pass_td_pct = Column(Float)
    ypa = Column(Float)
    rec_td_pct = Column(Float)
    yptarget = Column(Float)
    ayptarget = Column(Float)
    ypr = Column(Float)
    rush_td_pct = Column(Float)
    ypc = Column(Float)
    touches = Column(Integer)
    total_tds = Column(Integer)
    td_pct = Column(Float)
    total_yards = Column(Float)
    yptouch = Column(Float)
    offense_snaps = Column(Integer)
    offense_pct = Column(Float)
    team_offense_snaps = Column(Integer)
    solo_tackle = Column(Integer)
    assist_tackle = Column(Integer)
    tackle_with_assist = Column(Integer)
    sack = Column(Float)
    qb_hit = Column(Integer)
    def_touchdown = Column(Integer)
    defensive_two_point_attempt = Column(Integer)
    defensive_two_point_conv = Column(Integer)
    defensive_extra_point_attempt = Column(Integer)
    defensive_extra_point_conv = Column(Integer)
    defense_snaps = Column(Integer)
    defense_pct = Column(Float)
    team_defense_snaps = Column(Integer)
    safety = Column(Integer)
    interception = Column(Integer)
    fumble = Column(Integer)
    fumble_lost = Column(Integer)
    fumble_forced = Column(Integer)
    fumble_not_forced = Column(Integer)
    fumble_out_of_bounds = Column(Integer)

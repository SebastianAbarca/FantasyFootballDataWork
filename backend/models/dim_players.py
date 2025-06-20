from sqlalchemy import Column, String, Integer, Float
from backend.db import Base

class DimPlayers(Base):
    __tablename__ = "DimPlayers"

    player_id = Column(String(50), primary_key=True)
    player_name = Column(String(255), nullable=False)
    position = Column(String(10))
    birth_year = Column(Integer)
    draft_year = Column(Integer)
    draft_round = Column(Integer)
    draft_pick = Column(Integer)
    draft_ovr = Column(Integer)
    height = Column(Float)
    weight = Column(Float)
    college = Column(String(255))
    offense_defense_flag = Column(String(5))
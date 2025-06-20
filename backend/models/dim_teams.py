from backend.db import Base
from sqlalchemy import Column, String

class DimTeams(Base):
    __tablename__ = 'DimTeams'
    team_id = Column(String(10), primary_key=True)
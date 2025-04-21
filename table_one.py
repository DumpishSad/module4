from datetime import datetime, timezone

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, Float
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
from dotenv import load_dotenv
import os

Base = declarative_base()


class SpimexTradingResult(Base):
    __tablename__ = 'spimex_trading_result'
    id = Column(Integer, primary_key=True)
    exchange_product_id = Column(String, nullable=False)
    exchange_product_name = Column(String, nullable=False)
    oil_id = Column(String, nullable=False)
    delivery_basis_id = Column(String, nullable=False)
    delivery_basis_name = Column(String, nullable=False)
    delivery_type_id = Column(String, nullable=False)
    volume = Column(Float, nullable=False)
    total = Column(Float, nullable=False)
    count = Column(Integer, nullable=False)
    date = Column(DateTime, nullable=False)
    created_on = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_on = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, TypeDecorator
from typing import Any
from sqlalchemy.orm import relationship, declared_attr
from poros.dataModel.alchemist.BaseData import Base
from decimal import Decimal
import numpy as np
import math


class FloatOrNone(TypeDecorator):
    impl = Float

    def process_bind_param(self, value, dialect) -> Any:
        if isinstance(value, (float, Decimal, np.float64, np.float32)) and math.isnan(float(value)):
                return None
        return value

@auto_repr
class TimeSeriesSpec(Base):
    __tablename__ = 'time_series_spec'

    uid = Column(Integer, primary_key=True, index=True)
    provider = Column(String(250), nullable=True)
    ticker = Column(String(50), nullable=True)
    region = Column(String(100), nullable=True)
    name = Column(String(255), nullable=True)
    category = Column(String(50), ForeignKey('category_table_mapping.category'), nullable=False)
    datasource = Column(String(50), nullable=True)
    symbol = Column(String(50), nullable=True)

    __mapper_args__ = {'polymorphic_identity': 'time_series_spec'}


@auto_repr
class TimeSeries(Base):
    __abstract__ = True

    @declared_attr
    def uid(cls):
        return Column(Integer, ForeignKey('time_series_spec.uid'), primary_key=True, index=True)

    @declared_attr
    def date(cls):
        return Column(DateTime, primary_key=True)

    __mapper_args__ = {'polymorphic_identity': 'time_series'}


@auto_repr
class BondIndexSpec(TimeSeriesSpec):
    __tablename__ = 'bond_index_spec'

    uid = Column(Integer, ForeignKey('time_series_spec.uid'), primary_key=True, index=True)
    pricing_currency = Column(String(3), nullable=False, index=True)

    sector = Column(String(20), nullable=False)
    rating = Column(String(10), nullable=False)
    maturity_band = Column(String(20), nullable=False)
    maturity = Column(Integer, nullable=True)


    __mapper_args__ = {'polymorphic_identity': 'bond_index_spec'}

@auto_repr
class BondIndex(TimeSeries):
    __tablename__ = 'bond_index'

    uid = Column(Integer, ForeignKey('bond_index_spec.uid'), index=True, primary_key=True)
    date = Column(DateTime, primary_key=True)

    DM = Column(FloatOrNone, nullable=True)
    RI = Column(FloatOrNone, nullable=True)
    RY = Column(FloatOrNone, nullable=True)
    CX = Column(FloatOrNone, nullable=True)
    IN = Column(FloatOrNone, nullable=True)
    YTW = Column(FloatOrNone, nullable=True)

    __mapper_args__ = {'polymorphic_identity': 'bond_index'}
    _spec = relationship("BondIndexSpec", foreign_keys=[uid])



if __name__ == "__main__":

    import datetime
    from epsilonPhi.core.dataModel.alchemist.SessionManager import SessionMgr
    session = SessionMgr().getSessionFactory()

    try:
        session.query(ImpliedVolatility).filter(ImpliedVolatility.relative_strike.in_(['Spot', 'ATMF'])).update(
            {"relative_strike": "100"})
        session.commit()
        print("UPDATE successful.")
    except Exception as e:
        session.rollback()
        print("Error occurred during UPDATE:", str(e))
    finally:
        session.close()

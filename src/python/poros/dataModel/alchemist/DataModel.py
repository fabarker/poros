from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, TypeDecorator, LargeBinary
from typing import Any
from sqlalchemy.orm import relationship, declared_attr
from sqlalchemy.orm import declarative_base
from poros.lib.Decorators import auto_repr
from decimal import Decimal
import numpy as np
import math

__author__ = 'barkfr'

Base = declarative_base()

class FloatOrNone(TypeDecorator):
    impl = Float

    def process_bind_param(self, value, dialect) -> Any:
        if isinstance(value, (float, Decimal, np.float64, np.float32)) and math.isnan(float(value)):
                return None
        return value

@auto_repr
class TimeSeriesSpec(Base):

    __tablename__ = 'time_series_spec'
    __table_args__ = {'extend_existing': True}

    series_id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(50), nullable=True)
    name = Column(String(250), nullable=True)
    series_type = Column(String(250), nullable=True)
    units = Column(String(250), nullable=True)
    adjustment = Column(String(250), nullable=False)
    meta = Column(LargeBinary, nullable=True)

    __mapper_args__ = {'polymorphic_identity': 'time_series_spec'}

@auto_repr
class TimeSeries(Base):

    __tablename__ = 'time_series'
    __table_args__ = {'extend_existing': True}

    series_id = Column(Integer, ForeignKey('time_series_spec.series_id'), index=True, primary_key=True)
    symbol = Column(String(50), index=True, primary_key=True)
    date = Column(String(15), primary_key=True)
    close = Column(FloatOrNone, nullable=True)

    __mapper_args__ = {'polymorphic_identity': 'time_series'}
    _spec = relationship("TimeSeriesSpec", foreign_keys=[series_id])



if __name__ == "__main__":

    import datetime
    from poros.dataModel.alchemist.SessionManager import SessionManager
    session = SessionManager().session

    spec = session.query(TimeSeriesSpec).all()


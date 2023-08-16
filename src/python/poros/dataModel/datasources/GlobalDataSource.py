import pandas as pd
from poros.lib.Decorators import SingletonDecorator
from poros.dataModel.alchemist.DataModel import *
from poros.dataModel.alchemist.SessionManager import SessionManager
from poros.utils.DateUtils import DateUtils

__author__ = 'barkfr'

@SingletonDecorator
class GlobalDataSource(object):
    _session_mgr = SessionManager()
    _time_series_cache = pd.DataFrame()

    def __init__(self):
        pass

    @property
    def session(self):
        return self._session_mgr.session

    @property
    def engine(self):
        return self._session_mgr.engine

    def getTimeSeriesInfo(self, symbol: str):
        return gds.session.query(TimeSeriesSpec).filter(TimeSeriesSpec.symbol == symbol).all()

    def getTimeSeriesData(self, symbol=None, seriesID=None):
        df_series = self.getTimeSeriesDataFromSeriesID(seriesID)
        df_symbol = self.getTimeSeriesDataFromSymbol(symbol)
        return pd.concat((df_series, df_symbol), axis=1)

    def getTimeSeriesDataFromSeriesID(self, seriesID=None):

        if seriesID is None:
            return pd.DataFrame()

        if not DateUtils.is_iterable(seriesID):
           seriesID = [seriesID]

        q = gds.session.query(TimeSeries).filter(TimeSeries.series_id.in_(seriesID))
        df = self._session_mgr.query_format_df(q)
        return df.copy()

    def getTimeSeriesDataFromSymbol(self, symbol=None):

        if symbol is None:
            return pd.DataFrame()

        if not DateUtils.is_iterable(symbol):
           symbol = [symbol]

        q = gds.session.query(TimeSeries).filter(TimeSeries.symbol.in_(symbol))
        df = self._session_mgr.query_format_df(q)
        return df.copy()


if __name__ == "__main__":

    gds = GlobalDataSource()

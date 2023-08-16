#from epsilonPhi.core.env.Env import DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, DB_DRIVER, DB_DATABASE_NAME
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from poros.dataModel.alchemist.DataModel import *
from contextlib import contextmanager
import pandas as pd

DB_HOSTNAME = 'poros.mysql.database.azure.com'
DB_USERNAME = 'porosadmin'
DB_PASSWORD = 'Isgrocks1!'
DB_DRIVER = 'mysql+mysqlconnector'
DB_DATABASE_NAME = 'poros-dev'

class SessionManager(object):

    def __init__(self):
        self.initalize()

    @property
    def session(self):
        return self._Session

    @property
    def session_factory(self):
        return self._session_factory

    @property
    def engine(self):
        return self._engine

    def initalize(self):
        self.create_engine(DB_DATABASE_NAME)

    def get_connection_string(self, database_name=DB_DATABASE_NAME):
        return DB_DRIVER + '://' + DB_USERNAME +\
            ':' + DB_PASSWORD + '@' +\
            DB_HOSTNAME + '/' + database_name

    def create_engine(self, database_name):

        self._engine = create_engine(self.get_connection_string(database_name))
        Base.metadata.create_all(self._engineCache[database_name])
        self._session_factory = sessionmaker(bind=self._engineCache[database_name])
        self._Session = scoped_session(self._session_factory)

    def get_ticker_from_uid(self, uid: int) -> str:
        from poros.dataModel.alchemist.DataModel import TimeSeriesSpec
        return self.getSessionFactory().query(TimeSeriesSpec).filter_by(uid=uid).first().ticker

    def get_uid_from_ticker(self, ticker: str) -> int:
        from poros.dataModel.alchemist.DataModel import TimeSeriesSpec
        return self.getSessionFactory().query(TimeSeriesSpec).filter_by(ticker=ticker).first().uid

    @staticmethod
    def query_format_df(query):
        q = query.statement.compile(compile_kwargs={"literal_binds": True}).string
        return pd.read_sql(q.replace('"', ''), query.session.get_bind())

    def get_dataframe_from_uid(self, uid: int):
        class_ = self.fetch_model_class_from_uid(uid)
        query = self.getSessionFactory().query(class_).filter(class_.uid.in_([uid]))
        return self.query_format_df(query)

    def get_time_series_spec_from_uid(self, uid: str, return_df=False):

        q = self.getSessionFactory().query(TimeSeriesSpec).filter(TimeSeriesSpec.uid == uid)

        if return_df:
           return self.query_format_df(q)
        else:
           return q.first()

    def get_time_series_spec_from_ticker(self, ticker: str, return_df=False):

        q = self.getSessionFactory().query(TimeSeriesSpec).filter(TimeSeriesSpec.ticker == ticker)

        if return_df:
           return self.query_format_df(q)
        else:
           return q.first()

@contextmanager
def session_scope():
    scoped_session = SessionManager().session_factory
    session = scoped_session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":

    session = SessionManager().session_factory

    SQL_1 = "SELECT * FROM `epsilon-phi-dev`.time_series_spec where name like '%Spot%' and provider = 'GS' and category = 'Implied Volatility'"
    SQL_2 = "SELECT * FROM `epsilon-phi-dev`.time_series_spec where name like '%ATMF%' and provider = 'GS' and category = 'Implied Volatility'"

    df_ATM = pd.read_sql(SQL_2, session.get_bind())
    df_Spt = pd.read_sql(SQL_2, session.get_bind())
    df = pd.concat((df_ATM, df_Spt), axis=0)

    table_name = '`epsilon-phi-dev`.implied_volatility'
    unique_uids = df_ATM.uid.unique()
    N = len(unique_uids)

    ctr = 0
    for uid in df_ATM.uid.unique():

        print(N-ctr)
        READ_STATEMENT = f"SELECT * FROM {table_name} WHERE uid = {uid};"
        df_data = pd.read_sql(READ_STATEMENT, session.get_bind())
        df_data.relative_strike = '100'

        DELETE_STATEMENT = f"DELETE FROM {table_name} WHERE uid = {uid};"
        with session.get_bind().begin() as conn:
            conn.execute(text(DELETE_STATEMENT))

        df_data.to_sql('implied_volatility', session.get_bind(), if_exists='append', index=False)
        ctr = ctr + 1


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

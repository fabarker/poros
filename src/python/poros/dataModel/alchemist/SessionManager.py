from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from poros.lib.Decorators import SingletonDecorator
from poros.dataModel.alchemist.DataModel import *
from contextlib import contextmanager
import pandas as pd

__author__ = 'barkfr'

DB_HOSTNAME = 'poros.mysql.database.azure.com'
DB_USERNAME = 'porosadmin'
DB_PASSWORD = 'Isgrocks1!'
DB_DRIVER = 'mysql+mysqlconnector'
DB_DATABASE_NAME = 'poros-dev'

@SingletonDecorator
class SessionManager(object):

    def __init__(self):
        self.initalize()

    @property
    def session(self):
        return self.__Session

    @property
    def session_factory(self):
        return self.__session_factory

    @property
    def engine(self):
        return self.__engine

    def initalize(self):
        self.create_engine(DB_DATABASE_NAME)

    def get_connection_string(self, database_name=DB_DATABASE_NAME):
        return DB_DRIVER + '://' + DB_USERNAME +\
            ':' + DB_PASSWORD + '@' +\
            DB_HOSTNAME + '/' + database_name

    def create_engine(self, database_name):

        self.__engine = create_engine(self.get_connection_string(database_name))
        Base.metadata.create_all(self.__engine)
        self.__session_factory = sessionmaker(bind=self.__engine)
        self.__Session = scoped_session(self.__session_factory)

    @staticmethod
    def query_format_df(query):
        q = query.statement.compile(compile_kwargs={"literal_binds": True}).string
        return pd.read_sql(q.replace('"', ''), query.session.get_bind())

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



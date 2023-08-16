import numpy as np
from poros.dataModel.alchemist.SessionManager import SessionManager
from poros.utils.ListUtils import ListUtils
import dill, os, requests
import pandas as pd

__author__ = 'barkfr'

_USERNAME = ''
_PASSWORD = ''


class GlobalFinancialData(object):
    _url_login = 'https://api.globalfinancialdata.com/login/'
    _url_series = 'https://api.globalfinancialdata.com/series'

    def __init__(self, username=None, password=None):

        if username is None:
            self._username = _USERNAME
        else:
            self._username = username

        if password is None:
            self._password = _PASSWORD
        else:
            self._password = password
        self._parameters = {'username': self._username,
                            'password': self._password}
        self.get_token()

    def get_token(self):

        resp = requests.post(GlobalFinancialData._url_login, data=self._parameters)

        # check for unsuccessful API returns
        if resp.status_code != 200:
            raise ValueError('GFD API request failed with HTTP status code %s' % resp.status_code)

        json_content = resp.json()
        os.environ['GFD_API_TOKEN'] = json_content['token'].strip('"')

    def get_data(self, symbols):

        if isinstance(symbols, str):
            symbols = [symbols]
        symbol = ','.join(symbols)

        pars = {'token': os.environ['GFD_API_TOKEN'],
                      'seriesname': symbol,
                      'periodicity': 'Daily',
                      'closeonly': True,
                      'totalreturn': False,
                      'metadata': True,
                      }

        # series API call...
        r = requests.post(GlobalFinancialData._url_series, data=pars)
        if r.status_code == 200:
            data = pd.DataFrame(r.json()['price_data'])
            info = pd.DataFrame(r.json()['data_information'])
            info = info.rename(columns={'metadata': 'meta'})
            data['symbol'] = info.set_index('series_id').loc[data.series_id].get('symbol').values
        else:
            print('Error: {}'.format(r.json().get('message')))
            data = pd.DataFrame()
            info = pd.DataFrame()
        return data, info


if __name__ == "__main__":

    # Get engine to connect to Azure SQL Database....
    eng = SessionManager().engine

    # Instantiate GFD API Class....
    gfd = GlobalFinancialData('francis.barker@gs.com', 'isg123')

    # Load GFD ticker info from Excel....
    gfd_info = pd.read_excel(os.path.join(r'C:\Users\fabar\repos\poros\src\resources', 'GFD Tickers.xlsx'), index_col=0)
    unique_tickers = gfd_info.index.unique()

    # Create a nested list of list, so we dont overload the API....
    nested_lists = ListUtils._nest_list(list(unique_tickers), 10)

    for sub_list in nested_lists:

        # Query data....
        df_dta, df_info = gfd.get_data(symbols=sub_list)
        if df_dta.size > 0:

            # Pickle the metadata (because it is a very large string)....
            df_info.meta = df_info.meta.apply(lambda x: dill.dumps(x))

            # Keep only the columns we care about....
            cols = ['series_id', 'symbol', 'name', 'series_type', 'units', 'adjustment', 'meta']
            df_info_db = df_info[list(np.intersect1d(cols, df_info.columns))]

            # Save data to sql database tables....
            df_info_db.to_sql('time_series_spec', eng, if_exists='append', index=False)
            df_dta.to_sql('time_series', eng, if_exists='append', index=False)






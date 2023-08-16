import requests
import pandas as pd
import os

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

    def is_token_valid(self):
        pass

    def get_data(self, symbols):

        if isinstance(symbols, str):
            symbols = [symbols]
        symbol = ','.join(symbols)

        pars = {'token': os.environ['GFD_API_TOKEN'],
                      'seriesname': symbol,
                      'periodicity': 'Daily',
                      'closeonly': True,
                      'totalreturn': False,
                      'metadata': 'full',
                      }

        # series API call
        r = requests.post(GlobalFinancialData._url_series, data=pars)
        if r.status_code == 200:
            data = pd.DataFrame(r.json()['price_data'])
            info = pd.DataFrame(r.json()['data_information'])
        else:
            print('Error:'.format(r.status_code))
            data = pd.DataFrame()
            info = pd.DataFrame()
        return data, info


if __name__ == "__main__":
    gfd = GlobalFinancialData('francis.barker@gs.com', 'isg123')
    df = gfd.get_data(symbols=['NKE','KO','AXP',
                                   'AAPL','DIS','BA',
                                   'CVX','INTC','XOM',
                                   'JNJ','MMM','PFE',
                                   'MCD','HD','CSCO'])
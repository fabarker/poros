import os, sys
import pandas as pd

path = '/Users/francisbarker/Desktop/GFD Data.csv'
df = pd.read_csv(path, index_col=0)

unique_types = df['Series Type'].unique()

subset = df.reset_index(drop=False).set_index('Series Type')

from pandas import ExcelWriter
save_path = '/Users/francisbarker/Desktop/GFD Categories'
for type in unique_types:
    subset.loc[type].to_csv(os.path.join(save_path,
                                         type.replace('-','').replace('/','').replace('  ',' ') + '.csv'))

import json

from kiteconnect import KiteConnect
import pandas as pd
import urllib3
import pickle
import statistics
import time

from kiteconnect import KiteConnect
import datetime



class DataBank:
    def __init__(self, access_token, api_key,from_date,to_date):
        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token=access_token)
        all_tradables=pd.read_csv('full_list.csv')
        self.dict_all_tradables=all_tradables.to_dict(orient='records')

        self.from_date,self.to_date=from_date,to_date
        print(self.dict_all_tradables)
        self.data={}
        mean_values = []
        update_tradables=[]
        for x in self.dict_all_tradables:
            print(x)
            while True:
                try:
                    time.sleep(0.5)
                    hist_data = pd.DataFrame(
                        self.kite.historical_data(x['instrument_token'], from_date=self.from_date,
                                                  to_date=self.to_date,
                                                  interval='day'))
                    x['mean_volume'] = statistics.mean(hist_data['volume'].tolist())
                    x['mean_price'] = statistics.mean(hist_data['close'].tolist())
                    x['mean_value'] = x['mean_price'] * x['mean_volume']
                    mean_values.append(x['mean_value'])
                    update_tradables.append(x)
                except Exception as e:
                    print(e)
                    continue
                break
        average_mean_value = sum(mean_values) / len(mean_values)
        updated=[]
        for y in update_tradables:

            if y['mean_value']>1.5*average_mean_value and y['mean_price']>25:
                updated.append(y)
        print(updated)
        tradable_df=pd.DataFrame(updated)
        tradable_df['margin']=tradable_df['mis_multiplier']
        tradable_df['symbol']=tradable_df['tradingsymbol']
        tradable_df=tradable_df[['instrument_token','symbol','margin']]
        tradable_df.to_csv('tradables.csv')




access_token = 'wRW31cTjmI3RmZBrrFf1Pg3C18MiexXe'
api_key = '7lwrah449p8wk3i0'
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token=access_token)
instrument_list=pd.DataFrame(kite.instruments(exchange=kite.EXCHANGE_NSE))
instrument_list=instrument_list[instrument_list['segment'] == 'NSE']
instrument_list=instrument_list.set_index('tradingsymbol')

http = urllib3.PoolManager()
r = http.request('GET', 'https://api.kite.trade/margins/equity')
margins=pd.DataFrame(json.loads(r.data.decode('utf-8')))
margins=margins.set_index('tradingsymbol')
joined_df=pd.concat([instrument_list,margins],axis=1,join='inner')
joined_df=joined_df[['instrument_token','mis_multiplier']]
joined_df=joined_df.reset_index()
joined_df.to_csv('full_list.csv')
from_date=datetime.date.today() + datetime.timedelta(-30)
to_date=datetime.date.today()
print(from_date,to_date)

DataBank(access_token,api_key,from_date,to_date)


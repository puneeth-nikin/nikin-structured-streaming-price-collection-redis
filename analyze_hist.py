import datetime

import pandas as pd
import numpy as np

data=pd.read_csv('hist_20_days_data.csv')

data=data.groupby('symbol').apply(lambda x:x.sort_values(by=['date'])).reset_index(drop=True)
print(data)
data['sd_high']=data.groupby('symbol')['high'].rolling(10).std().reset_index(drop=True)
data['sd_low']=data.groupby('symbol')['low'].rolling(10).std().reset_index(drop=True)
data['range']=data['high']-data['low']
data['atr']=data.groupby('symbol')['range'].rolling(3).std().reset_index(drop=True)
data['natr']=(data['atr']/data['close'])*100


data['sd']=data[['sd_high','sd_low']].max(axis=1)




data=data.set_index([pd.DatetimeIndex(data['date'])])
data['date_only']=data.index.date

sum_df=data.groupby(['symbol','date_only'])['sd'].sum().reset_index().rename(columns={'sd':'sd_sum'})
sum_df['sd_sum']=sum_df.groupby('symbol')['sd_sum'].shift(1)
sum_df=sum_df.set_index(['symbol','date_only'])

count_df=data.groupby(['symbol','date_only'])['sd'].count().reset_index().rename(columns={'sd':'sd_count'})
count_df['sd_count']=count_df.groupby('symbol')['sd_count'].shift(1)
count_df=count_df.set_index(['symbol','date_only'])

data=data.reset_index(drop=True).set_index(['symbol','date_only'])
data['sd_sum']=sum_df
data['sd_count']=count_df

data=data.dropna()
data=data.reset_index()
data=data.set_index('date')
print(data)
data['avg_sd']=data['sd_sum']/data['sd_count']
data=data[data['sd']>4*data['avg_sd']]
data=data[data['natr']>0.3]
data=data[data['date_only'] == (datetime.date.today()+ datetime.timedelta(-8))]
data=data[['symbol','sd','avg_sd','natr']]
data.to_csv('active.csv')


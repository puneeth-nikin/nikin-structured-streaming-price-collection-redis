import json

from kiteconnect import KiteConnect
from kafka import KafkaProducer
import pandas as pd
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
from kafka import KafkaProducer
from pandas.io.json import json_normalize
import numpy as np

access_token = '0coZ2HYCFHpcNbxEj0Kk7QDrSN6lwSYv'
api_key = '7lwrah449p8wk3i0'

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token=access_token)
while True:
    try:
        orders = kite.orders()
        orders_df=pd.DataFrame(orders)
        orders_df=orders_df[['average_price','cancelled_quantity','disclosed_quantity','filled_quantity','instrument_token','order_id','order_timestamp','pending_quantity','quantity','status_message','tradingsymbol','transaction_type','status']]
        orders_df['order_id']=pd.to_numeric(orders_df['order_id'])
        orders_df['order_timestamp']=pd.to_datetime(orders_df['order_timestamp']).apply(lambda z: z.strftime('%Y-%m-%d %H:%M:%S'))
        orders_df=orders_df.rename(columns={'tradingsymbol':'symbol'})
        orders_df.to_csv('orders_df.csv')
        for x in orders_df.to_dict(orient="records"):
            producer.send('status', json.dumps(x).encode('utf-8'))
        orders_df.info()
    except Exception as e:
        print(e)
        pass


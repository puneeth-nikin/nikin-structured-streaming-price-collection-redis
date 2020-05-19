import datetime
import json
import time

import pandas as pd
from kafka import KafkaProducer
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker


def database_starter(kite, from_date, to_date):
    def additional_ticks(row):
        row['date'] = row['date'].strftime(('%Y-%m-%d %H:%M:%S'))
        row['mode'] = 'ltp'
        row['tradable'] = True
        list_tick = ['open', 'high', 'low', 'close']
        for x in list_tick:
            message = row[['instrument_token', 'symbol', 'mode', 'tradable', 'avg(sd)', 'date']]
            message['last_price'] = row[x]
            message_dict = message.to_dict()
            producer.send('trigger', json.dumps(message_dict).encode('utf-8'))

    df_todays_companies = pd.read_csv('tradables.csv')
    dict_todays_companies = df_todays_companies.to_dict(orient='records')

    subscription = []
    sym_tok_db = {}
    avg_sd_dict = {}

    for company in dict_todays_companies:
        print(company)
        list_requirements = ['instrument_token', 'margin', 'symbol']
        company_base = {}
        subscription.append(company['instrument_token'])
        for x in list_requirements:
            company_base[x] = company[x]

        company = company_base
        sym_tok_db[company['instrument_token']] = company['symbol']
        while True:
            try:
                company_data = pd.DataFrame(
                    kite.historical_data(company['instrument_token'], from_date=from_date,
                                         to_date=to_date,
                                         interval='minute'))
            except Exception as e:
                print(e)
                continue
            break
        company_data.drop('volume', axis=1)
        company_data['symbol'] = company['symbol']
        company_data['instrument_token'] = company['instrument_token']
        company_data['sd_high'] = company_data['high'].rolling(10).std()
        company_data['sd_low'] = company_data['low'].rolling(10).std()
        company_data['sd'] = company_data[['sd_high', 'sd_low']].max(axis=1)
        company_data['avg(sd)'] = company_data['sd'].mean()
        company_data = company_data.set_index('date')
        company_data = company_data.sort_index()
        company_data = company_data.reset_index()
        required_tick_data = company_data
        required_tick_data = required_tick_data[-30:]
        required_tick_data.apply(lambda row: additional_ticks(row), axis=1)
        avg_sd_dict[company['instrument_token']] = company_data['sd'].mean()

    add_data_df = pd.DataFrame.from_dict(
        {'symbol': list(sym_tok_db.values()), 'instrument_token': list(sym_tok_db.keys())})
    add_data_df = add_data_df.set_index('instrument_token')
    avg_sd_df = pd.DataFrame.from_dict(
        {'avg(sd)': list(avg_sd_dict.values()), 'instrument_token': list(avg_sd_dict.keys())})
    avg_sd_df = avg_sd_df.set_index('instrument_token')
    add_data_df = pd.concat([add_data_df, avg_sd_df], axis=1, join_axes=[avg_sd_df.index])

    return subscription, add_data_df


def tick_source(api_key, access_token, subscription, additional_data_df):
    kws = KiteTicker(api_key=api_key, access_token=access_token)

    def on_ticks(ws, ticks):
        start = time.time()
        timestamp = datetime.datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S')
        pd_ticks = pd.DataFrame(ticks)
        pd_ticks = pd_ticks.set_index('instrument_token')
        pd_ticks = pd.concat([pd_ticks, additional_data_df], axis=1, join_axes=[pd_ticks.index]).reset_index()
        pd_ticks['date'] = timestamp
        dict_ticks = pd_ticks.to_dict(orient='records')

        for tick in dict_ticks:

            if tick['last_price'] != 0:
                print(tick)

                producer.send('trigger', json.dumps(tick).encode('utf-8'))

        end = time.time()
        print('time taken update->', start - end)

    def on_connect(ws, response):
        # Callback on successful connect.
        # Subscribe to a list of instrument_tokens (VMART and TATAMOTORS here).
        ws.subscribe(subscription)
        print('connect')

        # Set VMART to tick in `full` mode.
        ws.set_mode(ws.MODE_LTP, subscription)

    # Assign the callbacks.
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect

    # Infinite loop on the main thread. Nothing after this will run.
    # You have to use the pre-defined callbacks to manage subscriptions.
    kws.connect()


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    access_token = 'B5P1axPKgftWyfzNV2H3cdpnEJR8SXui'
    api_key = '7lwrah449p8wk3i0'
    from_date = datetime.date.today() + datetime.timedelta(-4)
    to_date = datetime.date.today()
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token=access_token)
    subscription, additional_data_df = database_starter(kite, from_date, to_date)

    tick_source(api_key, access_token, subscription, additional_data_df)

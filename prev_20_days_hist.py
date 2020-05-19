import datetime

import pandas as pd
from kiteconnect import KiteConnect




def analyse(instrument_token, symbol):
    from_date = datetime.date.today() + datetime.timedelta(-20)
    to_date = datetime.date.today()
    ohlc_data = pd.DataFrame(
        kite.historical_data(instrument_token, from_date=from_date,
                             to_date=to_date,
                             interval='minute'))
    ohlc_data['symbol'] = symbol
    return ohlc_data

if __name__ == "__main__":
    access_token = 'HInJAt6LHQW7HnxXEoqHzRU1bOnBjVOU'
    api_key = '7lwrah449p8wk3i0'
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token=access_token)
    all_companies=pd.read_csv('tradables.csv')
    complete_data=pd.DataFrame()
    for index,row in all_companies.iterrows():
        print(row)
        company_data=analyse(row['instrument_token'],row['symbol'])
        complete_data=complete_data.append(company_data,ignore_index=True)

    complete_data.to_csv('hist_20_days_data.csv')
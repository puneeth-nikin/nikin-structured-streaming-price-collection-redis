import pandas as pd
import json
stop_loss_df=pd.read_csv('tradables.csv')
list_symbols=stop_loss_df['symbol'].tolist()

stop_loss_dict={}
for x in list_symbols:
    stop_loss_dict[x]=pd.DataFrame(columns=['open','high','low','close']).to_json(orient='records')
print(stop_loss_dict)
df=pd.DataFrame({'data':[]})
df = df['data'].str.decode("utf-8")
print(df)

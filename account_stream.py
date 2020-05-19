from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType, BooleanType, \
    IntegerType
import pyspark.sql.functions as func
import pandas as pd
from kafka import KafkaProducer
import numpy as np
from kiteconnect import KiteConnect

"""
spark-submit --conf spark.sql.shuffle.partitions=4 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 account_stream.py
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
"""
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

access_token = 'xom9KKp6yx2eEqv5NAqsxGeuolKUuUqb'
api_key = '7lwrah449p8wk3i0'
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token=access_token)

account_df = pd.read_csv('tradables.csv')
account_df = account_df
account_df = account_df[['instrument_token', 'symbol', 'margin']]
account_df['position_type'] = None
account_df['position'] = False
account_df['order_id'] = None
account_df['filled_quantity'] = 0
account_df['quantity'] = 0
account_df = account_df.set_index('symbol')

available_margin = 200000
position_value = 100000


def executor(df, epoch_id):
    def entry_order(row):
        row['quantity'] = round(position_value / row['price'])
        order_id = None

        if row['position_type'] == 'short':
            order_id = kite.place_order(tradingsymbol=row['symbol'], exchange=kite.EXCHANGE_NSE,
                                        transaction_type=kite.TRANSACTION_TYPE_SELL,
                                        quantity=row['quantity'],
                                        product=kite.PRODUCT_MIS, variety=kite.VARIETY_REGULAR,
                                        order_type=kite.ORDER_TYPE_MARKET,
                                        validity=kite.VALIDITY_DAY)

        if row['position_type'] == 'long':
            order_id = kite.place_order(tradingsymbol=row['symbol'], exchange=kite.EXCHANGE_NSE,
                                        transaction_type=kite.TRANSACTION_TYPE_BUY,
                                        quantity=row['quantity'],
                                        product=kite.PRODUCT_MIS, variety=kite.VARIETY_REGULAR,
                                        order_type=kite.ORDER_TYPE_MARKET,
                                        validity=kite.VALIDITY_DAY)

        row = row[['instrument_token', 'symbol', 'margin', 'position_type', 'quantity']]
        row['position'] = True
        row['order_id'] = order_id
        row['filled_quantity'] = row['quantity']

        return row

    def exit_order(row):

        if row['filled_quantity'] < row['quantity']:
            if row['filled_quantity'] != 0:
                order_id = kite.modify_order(order_id=row['order_id'], variety=kite.VARIETY_REGULAR,
                                             quantity=row['filled_quantity'])
                if row['position_type'] == 'short':
                    order_id = kite.place_order(tradingsymbol=row['symbol'], exchange=kite.EXCHANGE_NSE,
                                                transaction_type=kite.TRANSACTION_TYPE_BUY,
                                                quantity=row['filled_quantity'],
                                                product=kite.PRODUCT_MIS, variety=kite.VARIETY_REGULAR,
                                                order_type=kite.ORDER_TYPE_MARKET,
                                                validity=kite.VALIDITY_DAY)

                if row['position_type'] == 'long':
                    order_id = kite.place_order(tradingsymbol=row['symbol'], exchange=kite.EXCHANGE_NSE,
                                                transaction_type=kite.TRANSACTION_TYPE_SELL,
                                                quantity=row['filled_quantity'],
                                                product=kite.PRODUCT_MIS, variety=kite.VARIETY_REGULAR,
                                                order_type=kite.ORDER_TYPE_MARKET,
                                                validity=kite.VALIDITY_DAY)
            else:
                order_id = kite.cancel_order(order_id=row['order_id'], variety=kite.VARIETY_REGULAR)

        row = row[
            ['instrument_token', 'symbol', 'margin']]

        row['position_type'] = None
        row['position'] = False
        row['order_id'] = None
        row['quantity'] = 0
        row['filled_quantity'] = 0
        return row

    global account_df, available_margin
    stream_df = df.toPandas()

    if not stream_df.empty:
        stream_df=stream_df.drop_duplicates(subset=['symbol'],keep='first')
        stream_df = stream_df.set_index('symbol')
        entry_df = stream_df[stream_df['type'] == 'entry']
        entry_account_df = account_df[['instrument_token', 'margin', 'position']]

        entry_df = pd.concat([entry_df, entry_account_df], axis=1, join='inner').reset_index()

        entry_df = entry_df[entry_df['position'] == False]
        if not entry_df.empty:

            entry_df['value'] = position_value / entry_df['margin']
            entry_df = entry_df.sort_values(by='value')
            entry_df['cumsum'] = entry_df['value'].cumsum()
            entry_df = entry_df[entry_df['cumsum'] < available_margin]

            available_margin = available_margin - entry_df['value'].sum()

            if not entry_df.empty:
                entry_df = entry_df.apply(lambda row: entry_order(row), axis=1)
                entry_df = entry_df[
                    ['order_id', 'position', 'position_type', 'margin', 'instrument_token', 'symbol', 'filled_quantity',
                     'quantity']]
                entry_df = entry_df.set_index('symbol')
                account_df = entry_df.combine_first(account_df)

        exit_df = stream_df[stream_df['type'] == 'exit']
        exit_df = exit_df[['type']]
        exit_account_df = account_df
        exit_account_df = exit_account_df[exit_account_df['position']]
        if not exit_df.empty or not exit_account_df.empty:
            exit_df = pd.concat([exit_df, exit_account_df], axis=1, join='inner').reset_index()
            exit_df['value'] = position_value / exit_df['margin']

            available_margin = available_margin + exit_df['value'].sum()
            exit_df = exit_df.apply(lambda row: exit_order(row), axis=1)
            exit_df = exit_df[
                ['order_id', 'position', 'position_type', 'margin', 'instrument_token', 'symbol', 'filled_quantity',
                 'quantity']]

            exit_df = exit_df.set_index('symbol')
            exit_df.info()

            account_df = exit_df.combine_first(account_df)
    pass


schema_orders = StructType([
    StructField("symbol", StringType(), True),
    StructField("position_type", StringType(), True),
    StructField("type", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("start", TimestampType(), True)])
read_orders_df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option('subscribe', 'orders') \
    .load()

new_orders_df = read_orders_df.selectExpr("CAST(value AS STRING)").select(
    func.from_json(func.col('value'), schema_orders).alias("tmp")).select(
    "tmp.*")
new_orders_df.writeStream.foreachBatch(executor).start().awaitTermination()

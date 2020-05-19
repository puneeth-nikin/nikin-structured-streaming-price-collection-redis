import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType, BooleanType
import pyspark.sql.functions as func
import pandas as pd
from kafka import KafkaProducer
import numpy as np

"""
spark-submit --conf spark.sql.shuffle.partitions=4 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 position_stream.py
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

account_df = pd.read_csv('tradables.csv')
account_df = account_df
account_df = account_df[['instrument_token', 'symbol', 'margin']]
account_df['position'] = False
account_df = account_df.set_index('symbol')
account_df['position_time'] = None
account_df['position_type'] = None

position_df = pd.DataFrame(
    columns=['open', 'high', 'low', 'close', 'symbol', 'start', 'controller', 'stop_loss', 'position_type',
             'controller_constant', 'lag_number', 'controller_sd', 'avg(sd)'])

position_df = position_df.set_index(['symbol', 'start'])


def batch_processor(df, epoch_id):
    def entry_order(row):
        message = row
        message = message[['symbol', 'price', 'position_type']]
        message['type'] = 'entry'
        message_dict = message.to_dict()
        producer.send('orders', json.dumps(message_dict).encode('utf-8'))
        row['position'] = True
        row['position_time'] = row['start']
        row['position_type'] = row['position_type']
        return row

    def exit_order(row):
        message = row
        message = message[['symbol']]
        message['type'] = 'exit'
        message['price'] = None
        message['position_type'] = None
        message_dict = message.to_dict()
        producer.send('orders', json.dumps(message_dict).encode('utf-8'))
        row['position'] = False
        row['position_time'] = None
        row['position_type'] = None
        return row

    stream_df = df.toPandas()

    if not stream_df.empty:
        global position_df, account_df

        # start_exit
        if not position_df.empty:
            exit_df = position_df.reset_index()
            exit_df = exit_df.loc[exit_df.groupby('symbol')['start'].idxmax()]
            exit_df = exit_df.set_index('symbol')
            exit_stream_df = stream_df[['symbol', 'close']]
            exit_stream_df = exit_stream_df.set_index('symbol')
            exit_stream_df = exit_stream_df.rename(columns={'close': 'last_price'})
            exit_df = pd.concat([exit_df, exit_stream_df], axis=1, join='inner')
            exit_df = exit_df.reset_index()
            exit_df['exit_trigger'] = np.where(exit_df['position_type'] == 'short',
                                               exit_df['last_price'] > exit_df['stop_loss'],
                                               exit_df['last_price'] < exit_df['stop_loss'])
            exit_position_df = exit_df[exit_df['exit_trigger']]

            if not exit_position_df.empty:
                exit_position_df = exit_position_df.apply(lambda row: exit_order(row), axis=1)
                update_account_exit = exit_position_df[
                    ['symbol', 'position', 'position_time', 'position_type']].set_index(
                    'symbol')
                account_df.update(update_account_exit)

                position_df = position_df.drop(index=exit_position_df['symbol'].values)

        # end_exit

        # entry_start
        entry_stream_df = stream_df.drop_duplicates(subset=['symbol'], keep='last')
        entry_stream_df = entry_stream_df.set_index('symbol')
        prev_account_df = account_df[['margin', 'position', 'position_time']]

        new_account_df = pd.concat([prev_account_df, entry_stream_df], axis=1,
                                   join='inner')

        entry_account_df = new_account_df[new_account_df['position'] == False]

        entry_account_df = entry_account_df[entry_account_df['trigger']]
        entry_account_df = entry_account_df.reset_index()

        entry_account_df = entry_account_df.apply(lambda row: entry_order(row), axis=1)

        entry_account_df = entry_account_df[
            ['instrument_token', 'symbol', 'margin', 'position', 'position_time', 'position_type']]

        entry_account_df = entry_account_df.set_index('symbol')

        account_df.update(entry_account_df)

        current_positions = account_df[account_df['position']]
        if not current_positions.empty:
            current_positions = current_positions[['position_type']]
            new_info = stream_df.set_index('symbol')
            new_info = new_info[
                ['open', 'high', 'low', 'close', 'start', 'initial_stop_loss',
                 'initial_controller', 'avg(sd)']]
            combine_data = pd.concat([current_positions, new_info], axis=1, join='inner')
            combine_data = combine_data[
                ['open', 'high', 'low', 'close', 'start', 'initial_stop_loss', 'initial_controller', 'position_type',
                 'avg(sd)']].reset_index().set_index(['symbol', 'start']).rename(
                columns={"initial_stop_loss": "stop_loss", "initial_controller": "controller"})

            new_current_position_df = combine_data.combine_first(position_df).reset_index()
            head_df = new_current_position_df.groupby('symbol').first().reset_index()

            head_df['lag_number'] = 9
            head_df['controller_constant'] = np.where(head_df['position_type'] == 'short',
                                                      (head_df['low'] - head_df['controller']) ** 2,
                                                      (head_df['high'] - head_df['controller']) ** 2)

            head_df['controller_sd'] = np.where(head_df['position_type'] == 'short',
                                                (head_df['low'] - head_df['controller']),
                                                (head_df['high'] - head_df['controller']))

            head_df = head_df.set_index(['symbol', 'start'])
            new_current_position_df = new_current_position_df.set_index(['symbol', 'start'])

            new_current_position_df.update(head_df)

            new_current_position_df = new_current_position_df.reset_index()

            new_current_position_df = new_current_position_df.groupby('symbol').apply(
                lambda x: x.sort_values('start')).reset_index(drop=True)

            new_current_position_df = new_current_position_df.set_index(['symbol', 'start'])
            new_current_position_df['prev_controller'] = new_current_position_df['controller'].groupby('symbol').shift(
                1)
            new_current_position_df['prev_stop_loss'] = new_current_position_df['stop_loss'].groupby('symbol').shift(
                1)
            new_current_position_df['prev_lag_number'] = new_current_position_df['lag_number'].groupby('symbol').shift(
                1)

            new_current_position_df['lag_number'] = np.where(new_current_position_df['prev_lag_number'].isna(), 9,
                                                             new_current_position_df['prev_lag_number'] - 1)

            new_current_position_df['lag_number'] = np.where(new_current_position_df['lag_number'] < 0, 0,
                                                             new_current_position_df['lag_number'])
            new_current_position_df['controller_constant'] = new_current_position_df['controller_constant'].fillna(
                method='ffill')

            new_current_position_df['controller'] = np.where(new_current_position_df['position_type'] == 'short',
                                                             np.where(new_current_position_df['lag_number'] != 9,
                                                                      new_current_position_df['prev_controller'] + ((
                                                                                                                            2 / 11) * (
                                                                                                                            new_current_position_df[
                                                                                                                                'high'] -
                                                                                                                            new_current_position_df[
                                                                                                                                'prev_controller'])),
                                                                      new_current_position_df['controller']),
                                                             np.where(new_current_position_df['lag_number'] != 9,
                                                                      new_current_position_df['prev_controller'] + ((
                                                                                                                            2 / 11) * (
                                                                                                                            new_current_position_df[
                                                                                                                                'low'] -
                                                                                                                            new_current_position_df[
                                                                                                                                'prev_controller'])),
                                                                      new_current_position_df['controller']))

            new_current_position_df['controller_price_diff_sq'] = np.where(
                new_current_position_df['position_type'] == 'short',
                (new_current_position_df['controller'] - new_current_position_df[
                    'low']) ** 2,
                (new_current_position_df['controller'] - new_current_position_df[
                    'high']) ** 2)
            new_current_position_df['cumsum_controller_price_diff_sq'] = new_current_position_df[
                'controller_price_diff_sq'].rolling(
                10).sum()
            new_current_position_df['cumsum_controller_price_diff_sq'] = np.where(
                pd.isna(new_current_position_df['cumsum_controller_price_diff_sq']),
                new_current_position_df['controller_price_diff_sq'].cumsum(),
                new_current_position_df['cumsum_controller_price_diff_sq'])
            new_current_position_df['controller_sd'] = (((new_current_position_df['lag_number'] *
                                                          new_current_position_df['controller_constant']) +
                                                         new_current_position_df[
                                                             'cumsum_controller_price_diff_sq']) / 10) ** 0.5
            new_current_position_df['sl_multiplier'] = np.where(
                new_current_position_df['controller_sd'] / new_current_position_df['avg(sd)'] < 9,
                2 / ((round(10 / (new_current_position_df['controller_sd'] / new_current_position_df['avg(sd)']))) + 1),
                0.9)
            new_current_position_df['stop_loss'] = np.where((new_current_position_df['position_type'] == 'short'),
                                                            np.where(new_current_position_df['lag_number'] != 9,
                                                                     new_current_position_df['prev_stop_loss'] +
                                                                     new_current_position_df['sl_multiplier'] * (
                                                                             new_current_position_df['high'] -
                                                                             new_current_position_df[
                                                                                 'prev_stop_loss']),
                                                                     new_current_position_df['stop_loss']),
                                                            np.where(new_current_position_df['lag_number'] != 9,
                                                                     new_current_position_df['prev_stop_loss'] +
                                                                     new_current_position_df['sl_multiplier'] * (
                                                                             new_current_position_df['low'] -
                                                                             new_current_position_df[
                                                                                 'prev_stop_loss']),
                                                                     new_current_position_df['stop_loss']))

            new_current_position_df = new_current_position_df[
                ['open', 'high', 'low', 'close', 'controller', 'stop_loss', 'position_type',
                 'controller_constant', 'lag_number', 'controller_sd', 'avg(sd)']]

            position_df = new_current_position_df.combine_first(position_df)

        print(position_df[['stop_loss', 'lag_number', 'position_type']])

    pass


schema = StructType([
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("start", TimestampType(), True),
    StructField("trigger", BooleanType(), True),
    StructField("price", DoubleType(), True),
    StructField("instrument_token", LongType(), True),
    StructField("symbol", StringType(), True),
    StructField("source", StringType(), True),
    StructField("position_type", StringType(), True),
    StructField("initial_stop_loss", DoubleType(), True),
    StructField("initial_controller", DoubleType(), True),
    StructField("avg(sd)", DoubleType(), True)
])

read_df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option('subscribe', 'position') \
    .load()

write_df = read_df.selectExpr("CAST(value AS STRING)").select(
    func.from_json(func.col('value'), schema).alias("tmp")).select(
    "tmp.*").writeStream.foreachBatch(batch_processor).start()
write_df.awaitTermination()

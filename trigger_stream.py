import json
import time

import numpy as np
import pyspark.sql.functions as func
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window

"""
spark-submit --conf spark.sql.shuffle.partitions=50 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 trigger_stream.py
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

schema = StructType([
    StructField("instrument_token", LongType(), True),
    StructField("symbol", StringType(), True),
    StructField("last_price", DoubleType(), True),
    StructField("mode", StringType(), True),
    StructField("tradable", StringType(), True),
    StructField("avg(sd)", DoubleType(), True),
    StructField("date", TimestampType(), True)])


def batch_processor(df, epoch_id):
    start = time.time()
    pandas_df = df.toPandas()

    if pandas_df.empty:
        pass
    else:
        pandas_df = pandas_df.groupby(pandas_df['symbol']).apply(lambda x: x.sort_values(by=['start'])).reset_index(
            drop=True)
        pandas_df = pandas_df.groupby(pandas_df['symbol']).tail(25).reset_index(drop=True)

        pandas_df['sd_high'] = pandas_df['high'].groupby(pandas_df['symbol']).rolling(10).std().reset_index(drop=True)
        print(pandas_df[['symbol','high','sd_high']])
        pandas_df['sd_low'] = pandas_df['low'].groupby(pandas_df['symbol']).rolling(10).std().reset_index(drop=True)

        pandas_df['sd'] = pandas_df[['sd_high', 'sd_low']].max(axis=1)
        pandas_df['mave_5'] = pandas_df['close'].groupby(pandas_df['symbol']).rolling(5).mean().reset_index(drop=True)
        pandas_df['mave_8'] = pandas_df['close'].groupby(pandas_df['symbol']).rolling(8).mean().reset_index(drop=True)
        pandas_df['mave_16'] = pandas_df['close'].groupby(pandas_df['symbol']).rolling(16).mean().reset_index(drop=True)
        pandas_df['range'] = pandas_df['high'] - pandas_df['low']
        pandas_df['atr'] = pandas_df['range'].groupby(pandas_df['symbol']).rolling(3).mean().reset_index(drop=True)
        pandas_df['natr'] = pandas_df['atr'] / pandas_df['close'] * 100
        pandas_df['macd_line'] = pandas_df['mave_8'] - pandas_df['mave_16']
        pandas_df['signal'] = pandas_df['macd_line'].groupby(pandas_df['symbol']).rolling(5).mean().reset_index(
            drop=True)
        pandas_df['macd'] = pandas_df['macd_line'] - pandas_df['signal']
        pandas_df['position_type'] = np.where(pandas_df['macd'] > 0, 'short', 'long')
        pandas_df['prev_position_type'] = pandas_df['position_type'].groupby(pandas_df['symbol']).shift(1).reset_index(
            drop=True)
        pandas_df['changed'] = np.where(pandas_df['prev_position_type'] != pandas_df['position_type'], 1, 0)
        pandas_df['group_id'] = pandas_df['changed'].groupby(pandas_df['symbol']).cumsum().reset_index(drop=True)
        pandas_df = pandas_df.set_index(['symbol', 'group_id'])
        pandas_df['local_maxima'] = pandas_df.groupby(['symbol', 'group_id'])['high'].transform(
            lambda a: a.max())
        pandas_df['local_minima'] = pandas_df.groupby(['symbol', 'group_id'])['low'].transform(
            lambda a: a.min())
        pandas_df=pandas_df.reset_index()

        pandas_df = pandas_df.groupby(pandas_df['symbol']).tail(1).reset_index(drop=True)

        pandas_df['sd_stretched'] = np.where(pandas_df['sd'] > (4 * pandas_df['avg(sd)']), True, False)
        pandas_df['atr_stretched'] = np.where(pandas_df['natr'] > 0.3, True, False)
        pandas_df['reversal'] = np.where(pandas_df['position_type'] == 'short',
                                         np.where(pandas_df['close'] < pandas_df['local_maxima'] - pandas_df['atr'],
                                                  True, False),
                                         np.where(pandas_df['close'] > pandas_df['local_minima'] + pandas_df['atr'],
                                                  True, False))
        pandas_df['trigger'] = np.where(pandas_df['reversal'] & pandas_df['atr_stretched'] & pandas_df['sd_stretched'],
                                        True, False)
        pandas_df['price'] = np.where(pandas_df['position_type'] == 'short',
                                      pandas_df['local_maxima'] - pandas_df['atr'],
                                      pandas_df['local_minima'] + pandas_df['atr'])
        pandas_df['initial_stop_loss'] = np.where(pandas_df['position_type'] == 'short',
                                                  pandas_df['price'] + (pandas_df['atr']*2),
                                                  pandas_df['price'] - (pandas_df['atr']*2))
        pandas_df['initial_controller'] = np.where(pandas_df['position_type'] == 'short',
                                                   pandas_df['price'] + pandas_df['sd_high'],
                                                   pandas_df['price'] - pandas_df['sd_low'])
        print(pandas_df[['symbol','avg(sd)','sd','sd_stretched','atr_stretched']])
        pandas_df['source'] = 'entry'
        pandas_df = pandas_df[['open', 'high', 'low', 'close',
                               'start', 'trigger', 'price', 'instrument_token',
                               'symbol', 'source', 'position_type', 'initial_stop_loss',
                               'initial_controller', 'avg(sd)']]

        pandas_df['start'] = pandas_df['start'].apply(lambda z: z.strftime('%Y-%m-%d %H:%M:%S'))

        dict_df = pandas_df.to_dict(orient='records')
        for x in dict_df:
            producer.send('position', json.dumps(x).encode('utf-8'))
    end = time.time()
    print('time taken update->', start - end)

    pass


""".select(func.col('key'),func.to_json(func.struct("*")).alias("value"))"""
read_df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option('subscribe', 'trigger') \
    .load()

# noinspection PyTypeChecker
new_df = read_df.selectExpr("CAST(value AS STRING)").select(
    func.from_json(func.col('value'), schema).alias("tmp")).select(
    "tmp.*")

new_df_ohlc = new_df.withWatermark("date", "2 seconds") \
    .groupBy('instrument_token', func.window(func.col('date'), "60 seconds")) \
    .agg(func.first(func.col('last_price')),
         func.max(func.col('last_price')),
         func.min(func.col('last_price')),
         func.last(func.col('last_price')),
         func.last(func.col('symbol')),
         func.last(func.col('avg(sd)'))) \
    .withColumnRenamed('first(last_price, false)', 'open') \
    .withColumnRenamed('max(last_price)', 'high') \
    .withColumnRenamed('min(last_price)', 'low') \
    .withColumnRenamed('last(last_price, false)', 'close') \
    .withColumnRenamed('last(symbol, false)', 'symbol') \
    .withColumnRenamed('last(avg(sd), false)', 'avg(sd)') \
    .select('instrument_token', 'window.start', 'window.end',
            'open', 'high', 'low', 'close', 'symbol', 'avg(sd)').writeStream.foreachBatch(batch_processor) \
    .outputMode('complete').start()

new_df_ohlc.awaitTermination()

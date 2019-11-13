"""
Created on 13/11/19

@author: David Wong
"""
import findspark
import os

import pyarrow.csv, pyarrow.parquet

findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


def connect_spark(app_name='David_Spark', master='10.80.88.241:7077', lib_path='/Data_Science/Ignite/libs'):
    arg = []
    for name in os.listdir(lib_path):
        if name.endswith('.jar'):
            if arg:
                arg.append(',')
            arg.append(f'{lib_path}/{name}')
    arg = ''.join(arg)

    conf = SparkConf().setAppName(app_name).setMaster(f'spark://{master}').set('spark.jars', arg)
    sc = SparkContext(conf=conf)
    ss = SparkSession(sc)

    ss.conf.set('spark.sql.execution.arrow.enabled', 'true')

    return sc, ss


def load_ignite_table(table, ss,
                      config=os.environ['IGNITE_HOME'] + '/config/default-config.xml'):
    ignite = ss.read.format('ignite').option('config', config).option('table', table).load()
    ignite.createOrReplaceTempView(table)
    return ignite


def save_ignite_table(table, df,
                      config=os.environ['IGNITE_HOME'] + '/config/default-config.xml',
                      primary_keys=None, save_mode='overwrite'):
    ignite = df.write.format('ignite') \
        .option('table', table) \
        .option('config', config) \
        .mode(save_mode)

    if primary_keys:
        ignite.option('primaryKeyFields', primary_keys)

    ignite.save()


def load_csv(csv_file, ss, delimiter=',', dir='/', view=True):
    if csv_file.endswith('.csv'):
        csv_file, _ = os.path.splitext(csv_file)

    input = ss.read.format('csv') \
        .option('header', 'true') \
        .option('delimiter', delimiter) \
        .load(f'{dir}{csv_file}.csv')

    if view:
        input.createOrReplaceTempView(csv_file)

    return input


def load_parquet(pq_file, ss, dir='/', view=True):
    if pq_file.endswith('.parquet'):
        pq_file, _ = os.path.splitext(pq_file)

    pq = ss.read.parquet(f'{dir}{pq_file}.parquet')

    if view:
        pq.createOrReplaceTempView(pq_file)

    return pq


def csv_to_parquet(csv_file, pq_file=None):
    if not csv_file.endswith('csv'):
        csv_file = f'{csv_file}.csv'

    if pq_file is None:
        pq_file, _ = '{}.parquet'.format(os.path.splitext(csv_file))
    elif not pq_file.endswith('.parquet'):
        pq_file = f'{pq_file}.parquet'

    table = pyarrow.csv.read_csv(csv_file)
    pyarrow.parquet.write_table(table, pq_file, flavor='spark')



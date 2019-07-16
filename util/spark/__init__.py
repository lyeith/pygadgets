#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Apr 08 13:52:29 2019

@author: david
"""
import findspark
import pandas as pd

__version__ = '3.16.0'
__all__ = [
    'connect_spark_sql', 'exec_spark_sql'
]

from pyspark import SparkContext, SparkConf, sql

from .sql_parser import extract_tables

__author__ = 'David Wong <david.wong.jm@outlook.com>'


def connect_spark_sql(app_name='JUPYTER'):
    spark_conf = SparkConf().setAppName(app_name)

    # for k, v in crd.spark_cassandra.items():
    #     spark_conf.set(k, v)

    sc = SparkContext(conf=spark_conf)
    spark = sql.SparkSession(sc)

    return sc, spark


def exec_spark_sql(query, session, cache=False, table_cache={}):
    tables = extract_tables(query)
    tables = [e for e in tables if e[0] != ('(')]
    tokens = query.split()

    table_set = set()

    for elem in tables:
        v = elem.split(' ')[0]
        if len(v.split('.')) == 2:
            table_set.add(v)

    replacement_dct = {}

    for elem in table_set:
        v = elem.split('.')
        table_id = '_'.join(v)
        replacement_dct[elem] = table_id

        if table_id in table_cache:
            continue

        table_cache[table_id] = session.read.format('org.apache.spark.sql.cassandra') \
            .option('keyspace', v[0]).option('table', v[1]).load()
        table_cache[table_id].createOrReplaceTempView(table_id)

        if cache:
            table_cache[table_id].cache()

    for k, elem in enumerate(tokens):
        if elem in replacement_dct:
            tokens[k] = replacement_dct[elem]

    query = ' '.join(tokens)

    return session.sql(query)


def _map_to_pandas(rdds):
    """ Needs to be here due to pickling issues """
    return [pd.DataFrame(list(rdds))]


def toPandas(df, n_partitions=None):
    """
    Returns the contents of `df` as a local `pandas.DataFrame` in a speedy fashion. The DataFrame is
    repartitioned if `n_partitions` is passed.
    :param df:              pyspark.sql.DataFrame
    :param n_partitions:    int or None
    :return:                pandas.DataFrame
    """
    if n_partitions is not None: df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns
    return df_pand

#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Jul 10 15:47:08 2019

@author: david
"""
import newrelic.hooks.database_mysqldb
import newrelic.hooks.database_psycopg2
import pymongo
import pymysql
import psycopg2
import psycopg2.extras
import psycopg2.extensions
import time

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import RetryPolicy
from cassandra.query import dict_factory
from datetime import timezone


def connect_cassandra(conf, cluster=None, session=None):

    if session is not None:
        try:
            if session.is_shutdown:
                raise Exception
            else:
                return cluster, session
        except:
            time.sleep(2)

    auth_provider = PlainTextAuthProvider(
        username=conf['username'],
        password=conf['password']
    )
    contact_points = conf['host']

    cluster = Cluster(
        auth_provider=auth_provider,
        contact_points=contact_points,
        execution_profiles={
            'LOCAL_ONE': ExecutionProfile(
                consistency_level=ConsistencyLevel.LOCAL_ONE,
                request_timeout=45.0,
                row_factory=dict_factory,
                retry_policy=RetryPolicy.RETRY
            ),
            'LOCAL_QUORUM': ExecutionProfile(
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                request_timeout=60.0,
                row_factory=dict_factory,
                retry_policy=RetryPolicy.RETRY
            ),
            'EACH_QUORUM': ExecutionProfile(
                consistency_level=ConsistencyLevel.EACH_QUORUM,
                request_timeout=60.0,
                row_factory=dict_factory,
                retry_policy=RetryPolicy.RETRY
            ),
            'ALL': ExecutionProfile(
                consistency_level=ConsistencyLevel.ALL,
                request_timeout=75.0,
                row_factory=dict_factory,
                retry_policy=RetryPolicy.RETRY
            )
        }
    )

    session = cluster.connect()
    return cluster, session


def cassandra_async_query(session, query, param_lst=None, execution_profile='EACH_QUORUM', batch_size=2000):

    ps = session.prepare(query)
    result = []

    if param_lst is None:
        return cassandra_simple_query(session, query, param_lst, execution_profile=execution_profile)

    param_meta_lst = [param_lst] if len(param_lst) <= batch_size else \
        [param_lst[i:i + batch_size] for i in range(0, len(param_lst), batch_size)]

    for param_sub_lst in param_meta_lst:
        async_lst = []
        for param in param_sub_lst:
            async_lst.append(session.execute_async(ps, param, execution_profile=execution_profile))

        for elem in async_lst:
            res = elem.result()
            result += res

    return result


def cassandra_simple_query(session, query, param=None, execution_profile='EACH_QUORUM'):

    result = list(session.execute(query, param, execution_profile=execution_profile))
    return result


def connect_mongodb(conf, timezone=None, server_timeout=10, socket_timeout=90):
    '''
    :param conf: dictionary (username, password, host, database)
    :return:
    '''

    client_c = pymongo.MongoClient('mongodb://{username}:{password}@{host}/{database}'.format(
        **conf.mongodb_cashloan), serverSelectionTimeoutMS=server_timeout * 1000, socketTimeoutMS=socket_timeout * 1000)
    client_c.admin.command('ismaster')

    res = client_c.admin.command('ismaster')
    db_time = res['localTime'].replace(tzinfo=timezone.utc)

    if timezone:
        db_time = db_time.astimezone(tz=timezone)

    return client_c, db_time


def connect_mysql(conf, timezone=None, autocommit=True, timeout=30):

    conv = pymysql.converters.conversions.copy()
    conv[246] = float

    conn_m = pymysql.connect(**conf.mysql_cashloan, conv=conv, read_timeout=timeout, autocommit=autocommit)
    exec_sql(conn_m, 'SET @@session.time_zone = \'+07:00\'', fetch=False)

    res = exec_sql(conn_m, 'SELECT NOW()')
    db_time = list(res[0].values())[0]

    if timezone:
        db_time = db_time.replace(tzinfo=timezone)

    return conn_m, db_time


def connect_postgres(conf, timezone=None, readonly=True, autocommit=True, timeout=15):

    conn_pa = psycopg2.connect(**conf.pg_atmdb, connect_timeout=timeout)

    conn_pa.set_session(readonly=readonly, autocommit=autocommit)
    exec_sql(conn_pa, 'SET TIMEZONE TO \'Asia/Jakarta\'', fetch=False)

    res = exec_sql(conn_pa, 'SELECT NOW()')
    db_time = list(res[0].values())[0]

    if timezone:
        db_time = db_time.replace(tzinfo=timezone)

    return conn_pa, db_time


def exec_sql(conn, query, param=None, fetch=True):

    if type(conn) in (pymysql.connections.Connection,
                      newrelic.hooks.database_mysqldb.ConnectionWrapper):
        cur = conn.cursor(pymysql.cursors.DictCursor)
    elif type(conn) in (psycopg2.extensions.connection,
                        newrelic.hooks.database_psycopg2.ConnectionWrapper):
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        cur = conn.cursor()

    if param:
        cur.execute(query, param)
    else:
        cur.execute(query)

    res = cur.fetchall() if fetch else []
    res = res if res else []

    cur.close()

    return res


def exec_sql_transaction(conn, query_lst, param_lst=None):

    conn.begin()

    if type(conn) in (pymysql.connections.Connection,
                      newrelic.hooks.database_mysqldb.ConnectionWrapper):
        cur = conn.cursor(pymysql.cursors.DictCursor)
    elif type(conn) in (psycopg2.extensions.connection,
                        newrelic.hooks.database_psycopg2.ConnectionWrapper):
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        cur = conn.cursor()

    try:
        for idx, query in enumerate(query_lst):
            if param_lst:
                cur.execute(query, param_lst[idx])
            else:
                cur.execute(query)
    except:
        conn.rollback()
        raise
    else:
        conn.commit()


def pg_insert_many_sql(conn, query, params):

    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, query, params)
    conn.commit()


def upsert_postgres_query(table, constraint, keys, variables):

    sql = list()
    sql.append('INSERT INTO %s AS a(' % table)
    sql.append(', '.join(keys + variables))
    sql.append(') VALUES (')
    sql.append(', '.join(['%({})s '.format(x, x) for x in (keys+ variables)]))
    sql.append(') ON CONFLICT ON CONSTRAINT %s DO UPDATE SET '%constraint)
    sql.append(', '.join(['{} = %({})s '.format(x,x) for x in variables]))
    sql.append('WHERE ')
    sql.append('AND '.join(['a.{} = %({})s '.format(x,x) for x in keys]))
    sql.append(';')

    return ''.join(sql)

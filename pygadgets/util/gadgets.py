#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Jul 10 15:49:52 2019

@author: david
"""
import collections
import cassandra.util
import datetime
import dateutil.parser
import simplejson as json
import time

from . import kafka_util
from flask.json import JSONEncoder
from functools import wraps


def _process_tuple_key(dct):
    cp_dct = dct.copy()
    cp_dct_new = {}
    for k, v in cp_dct.items():
        if isinstance(v, (dict, collections.defaultdict)):
            if isinstance(k, (tuple,)):
                cp_dct_new[str(k)] = _process_tuple_key(v)
            else:
                cp_dct_new[k] = _process_tuple_key(v)
        elif isinstance(k, (tuple,)):
            cp_dct_new[str(k)] = v
        else:
            cp_dct_new[k] = v
    return cp_dct_new


def json_deserial(dct):

    for elem in ('update_time', 'create_time'):
        if elem in dct:
            dct[elem] = dateutil.parser.parse(dct['update_time'])

    return dct


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    elif isinstance(obj, (cassandra.util.Date,)):
        return obj.date().isoformat()
    elif isinstance(obj, (cassandra.util.OrderedMapSerializedKey,)):
        return dict(obj)
    elif isinstance(obj, (cassandra.util.SortedSet, frozenset, set, tuple)):
        return list(obj)


def kafka_iterator(servers, topic=None, reset_offset=True, offset_type='end', group=None, blocking=True):
    if topic is None:
        raise Exception('Kafka Topic Required')

    if group is None:
        raise Exception('Kafka Group Required')

    consumer = kafka_util.KafkaConsumer(
        servers, topic, reset_offset=reset_offset, reset_type=offset_type, consumer_group=group)

    consumer.activate()

    i = 0
    while True:
        msg = None
        try:
            i += 1
            if i % 10 == 0:
                consumer.commit_offsets()
                i = 0
            msg = consumer.consume()
        except:
            consumer = kafka_util.KafkaConsumer(servers, topic, reset_offset=True)
            continue

        if not msg and blocking:
            time.sleep(2)
            continue
        else:
            yield None
            continue

        msg_raw = msg.value().decode('utf-8')

        yield json.loads(msg_raw)


def load_json(objc):
    return json.loads(objc, object_hook=json_deserial)


def multiprocess_list(lst, func, processes=8, stream=False):
    import multiprocessing
    pool = multiprocessing.Pool(processes=processes)

    look_ahead = processes * 2
    res = collections.deque()

    if type(lst) == list:
        lst = iter(lst)

    while True:

        buffer_deficit = look_ahead - len(res)
        if buffer_deficit > 0:
            for x in range(buffer_deficit):
                try:
                    arg = next(lst)
                    if arg:
                        res.append(pool.apply_async(func, [arg]))
                except:
                    pass

        if stream and len(res) == 0:
            continue

        elif len(res) == 0:
            break

        yield res.popleft().get()


def print_json(obj):
    objc = _process_tuple_key(obj) if isinstance(obj, (dict, collections.defaultdict)) else obj
    print(json.dumps(objc, indent=4, default=json_serial))


def retry(exceptions, tries=5, delay=30, backoff=1.5, logger=None):
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = '{}, Retrying in {} seconds...'.format(e, mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


class CustomerFlaskJSONEncoder(JSONEncoder):
    def default(self, obj):
        return json_serial(obj)

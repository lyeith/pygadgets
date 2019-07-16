#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Jul 10 15:54:23 2019

@author: david
"""
import logging
import simplejson as json
import sys
import time

from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING, OFFSET_END

logger = logging.getLogger('streaming.util.kafka_util')


class KafkaConsumer:

    def __init__(self, servers, topic, reset_offset=False, reset_type='start', consumer_group=None):
        self.bootstrap = servers

        self.consumer = None
        self.topic = topic
        self.reset_offset = reset_offset
        self.reset_type = reset_type
        self.consumer_group = consumer_group.lower().encode('utf-8')

    def activate(self):
        while True:
            try:
                conf = {
                    'bootstrap.servers': self.bootstrap,
                    'group.id': self.consumer_group,
                    'enable.auto.commit': False,
                    'auto.offset.reset': 'earliest'
                }
                self.consumer = Consumer(**conf)

                if self.reset_offset is True:
                    if self.reset_type == 'start':
                        tp = TopicPartition(self.topic, 0, OFFSET_BEGINNING)
                    else:
                        tp = TopicPartition(self.topic, 0, OFFSET_END)
                    self.consumer.assign([tp])
                else:
                    self.consumer.subscribe([self.topic])
                break

            except:
                time.sleep(30)
                logger.warning('Kafka Consumer {} Reconnected. Exception {}'.format(self.topic, sys.exc_info()))

    def consume(self):
        try:
            raw = self.consumer.consume(num_messages=1, timeout=0.3)
            return raw[0] if raw else None

        except:
            self.activate()
            raw = self.consumer.consume(num_messages=1, timeout=0.3)
            return raw[0] if raw else None

    def commit_offsets(self):
        try:
            self.consumer.commit()

        except:
            self.activate()

    def stop(self):
        self.consumer.close()


class KafkaProducer:

    def __init__(self, servers, topic):
        self.bootstrap = servers

        self.producer = None
        self.topic = topic

    def activate(self, reconnect=False):

        if reconnect or self.producer is None:
            try:
                conf = {
                    'bootstrap.servers': self.bootstrap
                }
                self.producer = Producer(**conf)

            except:
                time.sleep(30)
                logger.warning('Kafka Producer {} Reconnected. Exception {}'.format(self.topic, sys.exc_info()))

    def produce(self, json_msg):
        try:
            self.producer.produce(self.topic, json.dumps(json_msg).encode('utf-8'))

        except:
            self.activate(reconnect=True)
            self.producer.produce(self.topic, json.dumps(json_msg).encode('utf-8'))

    def stop(self):
        self.producer.flush()

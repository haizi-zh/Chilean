#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import time
import pymongo
from pymongo.errors import AutoReconnect

from utils.database import get_mongodb
from abstract_class import BaseWatcher
from pymongo.cursor import _QUERY_OPTIONS
from core import OPLOG_WATCHER
from utils import serialize,load_yaml
import pika
import bson


class OplogWatcher(BaseWatcher):
    """
    MongoDB oplog.rs Watcher
    """

    def __init__(self, name=OPLOG_WATCHER, profile=None, queue=None, connection=None):
        BaseWatcher.__init__(self, name=name)
        self.conf_all = load_yaml()
        self.profile=self.conf_all['midware'] if 'midware' in self.conf_all else {}
        self.host = self.profile['server']['host']
        self.port=self.profile['server']['port']
        self.user = self.profile['auth']['user']
        self.password = self.profile['auth']['passwd']
        # if collection is not None:
        # if db is None:
        # raise ValueError('must specify db if you specify a collection')
        #     self._ns_filter = db + '.' + collection
        # elif db is not None:
        #     self._ns_filter = re.compile(r'^%s\.' % db)
        # else:
        #     self._ns_filter = None


        self.connection = connection or get_mongodb(profile=profile)



    def send_message(self, message):
        temp_obj = {'name': self.name, 'msg': message}
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(self.host,
                                       self.port,
                                       '/',
                                       credentials)
        connection = pika.BlockingConnection(parameters=parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange='trigger_exch', type='fanout')
        channel.basic_publish(exchange='trigger_exch',
                              routing_key='',
                              body=serialize(temp_obj), properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
        connection.close()
        #print serialize(temp_obj)
        #print 'be sent'

    def op_info_generator(self):  # oplog抓取
        """
        generate oplog info
        """
        oplog = self.connection['local']['oplog.rs']
        last_oplog_ts = oplog.find().sort('$natural', -1)[0]['ts']
        #print last_oplog_ts
        # a = bson.Timestamp(1429113600, 1)  # 4.16
        #b = bson.Timestamp(1427817600, 1)  # 4.1


        #cursor = oplog.find({'ts': {'$gt': b}}, {'ts': 1},tailable=True).sort('$natural', pymongo.ASCENDING)
        #cursor.add_option(_QUERY_OPTIONS['oplog_replay'])

        #for op in cursor:
        #    print op
        _SLEEP = 10
        while True:
            query = {'ts': {'$gt': last_oplog_ts}}
            cursor = oplog.find(query, tailable=True)
            # 对oplog查询进行优化
            cursor.add_option(_QUERY_OPTIONS['oplog_replay'])

            try:
                while cursor.alive:
                    try:
                        for op in cursor:
                            # 写消息队列
                            print op
                            self.send_message(op)
                            last_oplog_ts = op['ts']
                    except (AutoReconnect, StopIteration):  # StopIteration是在循环对象穷尽所有元素时的异常
                        time.sleep(_SLEEP)

            finally:
                cursor.close()



    # while True:
    # # 筛选
    #     try:
    #         cursor = oplog.find({'ts': {'$gt': last_oplog_ts}}, {'ts':1}, tailable=True)
    #         cursor.add_option(_QUERY_OPTIONS['oplog_replay'])
    #         while True:
    #             # self.send_message(oplog.find_one())
    #             for op in cursor:
    #                 # 更新时间，用于意外重启后直接查找
    #                 last_oplog_ts = op['ts']
    #                 # 消息写入队列
    #                 print op
    #                 # self.send_message(op)
    #             if not cursor.alive:
    #                 break
    #     except (AutoReconnect, StopIteration):# StopIteration是在循环对象穷尽所有元素时的异常
    #         time.sleep(1)
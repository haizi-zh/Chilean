#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import time
from pymongo.errors import AutoReconnect
from utils.database import get_mongodb
from abstract_class import BaseWatcher
from core import OPLOG_WATCHER


class OplogWatcher(BaseWatcher):
    """
    MongoDB oplog.rs Watcher
    """
    def __init__(self, name=OPLOG_WATCHER, profile=None, queue=None, db=None, collection=None, connection=None):
        BaseWatcher.__init__(self, name=name, queue=queue)

        if collection is not None:
            if db is None:
                raise ValueError('must specify db if you specify a collection')
            self._ns_filter = db + '.' + collection
        elif db is not None:
            self._ns_filter = re.compile(r'^%s\.' % db)
        else:
            self._ns_filter = None

        self.connection = connection or get_mongodb(profile=profile)
        if self.connection is not None:
            print 'db connection success'

    def op_info_generator(self):
        """
        generate oplog info
        """
        oplog = self.connection['local']['oplog.rs']
        ts = oplog.find().sort('$natural', -1)[0]['ts']
        while True:
            # 筛选
            if self._ns_filter is None:
                mfilter = {}
            else:
                mfilter = {'ns': self._ns_filter}
            mfilter['ts'] = {'$gt': ts}
            try:
                cursor = oplog.find(mfilter, tailable=True)
                while True:
                    for op in cursor:
                        # 更新时间，用于意外重启后直接查找
                        ts = op['ts']
                        # 消息写入队列
                        self.send_message(op)
                    if not cursor.alive:
                        break
            except AutoReconnect:
                time.sleep(1)
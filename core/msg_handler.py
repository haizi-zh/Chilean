#!/usr/bin/env python
# -*- coding: utf-8 -*-

from core.abstract_class import AbstractSubject
from utils import deserialize
import os
import logging
from core import PROCESSOR_WATCHER, OPLOG_WATCHER
from Queue import Queue


class MsgHandler(AbstractSubject):
    """
    oplog msg handler
    """
    def __init__(self, msg_size=0, listener={}):
        self.listener = listener
        self.process_path = os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir, 'processor'))
        self.msg_queue = Queue(maxsize=msg_size)
        # self.msg_queue = None
        self.file_cls_map = {}  # file和processor的映射，一个file包含多个processor
        self.listener_init()


    def deregister(self, filename):
        try:
            map(lambda x: self.listener.pop(x), self.file_cls_map[filename])
            #map(lambda x:logging.info('%s deregister success' % x),self.file_cls_map[filename])
            logging.info('%s deregister success' % filename)
            print '%s deregister success' % filename
            print self.file_cls_map
        except KeyError:
             logging.info('%s deregister error' % filename)
            #map(lambda x:logging.info('%s deregister success' % x),self.file_cls_map[filename])

    def notify_listeners(self, event):
        for name in self.listener.keys():
            self.listener[name].update(event)

    def process(self, msg):
        # if msg['name'] == OPLOG_WATCHER:
        #     self.notify_listeners(msg['msg'])
        if msg['name'] == PROCESSOR_WATCHER:
            self.update_listener(msg['msg'])

    def update_listener(self, event):
        if event['type'] == 'a':
            filename = event['name']
            self.register(filename)

        elif event['type'] == 'd':
            filename = event['name']
            self.deregister(filename)
            # TODO complex

        elif event['type'] == 'm':
            pass     #
            # TODO complex

    def process_msg(self):
        print 'begin'
        while True:
            msg = self.msg_queue.get(block=True)
            self.process(deserialize(msg))

    def get_msg_queue(self):
        return self.msg_queue

    def listener_init(self):
        import types
        import re
        file_list = os.listdir(os.path.join(self.process_path, '.'))
        target_file = map(lambda x: x.split('.')[0], filter(lambda x: (not isinstance(re.search('.py$', x), types.NoneType) and x != '__init__.py'), file_list))
        for filename in target_file:
            self.register(filename)

    def get_listeners(self):
        return self.listener

    def register(self, filename):
        import types
        import imp
        try:
            ret = imp.find_module(filename, [self.process_path]) if self.process_path else imp.find_module(filename)
            mod = imp.load_module(filename, *ret)
        except (UnicodeEncodeError, TypeError, AttributeError, KeyError):
            logging.info('load %s error' % filename)
        self.file_cls_map[filename] = []
        for attr_name in dir(mod):
            try:
                target_cls = getattr(mod, attr_name)
                name = getattr(target_cls, 'name')
                func = getattr(target_cls, 'update')
                if isinstance(name, str) and isinstance(func, types.MethodType):
                    self.listener[name] = target_cls()
                    print "process %s initial success" % name
                    self.file_cls_map[filename].append(name)
                else:
                    continue
            except (TypeError, AttributeError):
                pass
#!/usr/bin/env python
# -*- coding: utf-8 -*-
from utils import serialize


class BaseWatcher(object):
    """
    Base Watcher inherited by both op-log watcher and message_watcher
    """

    def __init__(self, name='', queue=None):
        self.msg_queue = queue
        self.name = name

    def send_message(self, message):
        # TODO condition: queue is full
        temp_obj = {'name': self.name, 'msg': message}
        self.msg_queue.put(serialize(temp_obj))
        # self.msg_queue.put(temp_obj)

    def get_name(self):
        return self.name


class AbstractSubject(object):
    """
    Abstract Subject
    """

    def register(self, listener):
        raise NotImplementedError("Must subclass me")

    def unregister(self, listener):
        raise NotImplementedError("Must subclass me")

    def notify_listeners(self, event):
        raise NotImplementedError("Must subclass me")


class BaseProcessor(object):
    """
    Abstract Listener
    """
    def update(self, msg, db):
        raise NotImplementedError("Must subclass me")


class BaseTrigger(object):
    """
    Abstract Trigger
    """
    def update_data(self, msg, db=None):
        raise NotImplementedError("Must subclass me")
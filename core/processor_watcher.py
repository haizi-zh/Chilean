#!/usr/bin/env python
# -*- coding: utf-8 -*-


from abstract_class import BaseWatcher
import os
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from core import PROCESSOR_WATCHER, OPLOG_DELETE, OPLOG_ADD, OPLOG_MODIFY


class ProcessorWatcher(FileSystemEventHandler, BaseWatcher):
    """
    Observer Watcher
    """
    def __init__(self, name=PROCESSOR_WATCHER, queue=None):
        BaseWatcher.__init__(self, name=name, queue=queue)
        self.path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'processor'))
        self.listeners = {}

    def on_any_event(self, event):
        pass

    def on_moved(self, event):
        pass

    def on_created(self, event):
        class_name = self.__get_fname(event)
        self.send_message({'type': OPLOG_ADD, 'name': class_name})

    def on_deleted(self, event):
        class_name = self.__get_fname(event)
        self.send_message({'type': OPLOG_DELETE, 'name': class_name})

    def on_modified(self, event):
        class_name = self.__get_fname(event)
        self.send_message({'type': OPLOG_MODIFY, 'name': class_name})

    @staticmethod
    def __get_fname(event):
        return event.key[1].split('/')[-1].split('.')[0]


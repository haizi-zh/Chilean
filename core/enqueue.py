# -*- coding: utf-8 -*-
__author__ = 'wdx'
from core import tasks
from core import PROCESSOR_WATCHER
from core import OPLOG_WATCHER
from Queue import Queue





class Enqueue:
    def __init__(self):
        pass

    def put(self,msg):
        if msg['name'] == OPLOG_WATCHER: #判断如果是 oplog消息,添加到celery消息任务队列
            tasks.oplog.delay(msg)
        # elif msg['name'] == PROCESSOR_WATCHER: #如果是进程文件变化的消息,添加到Queue()当前的队列中
        #     self.queue.put(serialize(msg))
        print 'there is a task entering queue.'

    def get(self):
        # msg = self.queue.get(block=True) #获取当前的队列中的消息
        # return msg

        pass

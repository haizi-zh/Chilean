# encoding=utf-8
__author__ = 'wdx'
import os
print os.path.dirname(__file__)
from celery import Celery
from msg_handler import MsgHandler
from core.processor_watcher import ProcessorWatcher
from watchdog.observers import Observer
from threading import Thread


app = Celery('tasks',backend='amqp', broker='amqp://')

msg_handler = MsgHandler()
msg_queue = msg_handler.get_msg_queue()#得到消息队列

msg_handler_thread = Thread(target=msg_handler.process_msg)
msg_handler_thread.start() # 打开一个线程,等待（block）注册或处理事件

event_handler = ProcessorWatcher(queue=msg_queue)
observer = Observer()
observer.schedule(event_handler, event_handler.path, recursive=True)
observer.start() #监控processor中文件的变化

'''
对oplog 的事件处理
'''

@app.task
def oplog(msg): #传入oplog消息
    msg_handler.notify_listeners(msg)
    print 'oplog'




# encoding=utf-8
__author__ = 'wdx'
from celery import Celery


app = Celery('tasks',backend='amqp', broker='amqp://')


'''
对oplog 的事件处理
'''
@app.task()
def oplog(msg,listeners): #传入oplog消息和listeners的方法
    for name in listeners.keys():
        listeners[name].update(msg['msg'])
    print 'oplog'

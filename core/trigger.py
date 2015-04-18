# encoding=utf-8
__author__ = 'bxm'
import logging
import pika
from utils import deserialize
from core.abstract_class import BaseTrigger


class Trigger(BaseTrigger):

    def __init__(self):
        pass


    def process_message(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters('119.254.100.93'))
        channel = connection.channel()
        channel.exchange_declare(exchange='trigger_exch',type='fanout')
        result = channel.queue_declare(exclusive=True) # 跟consumer失去联系时删除队列
        queue_name = result.method.queue
        channel.queue_bind(exchange='trigger_exch',
                    queue=queue_name)

        def callback(ch, method, properties, body):
            try:
                msg = deserialize(body)
                op = msg['msg']['op']
                doc = msg['msg']['o']
                ns=msg['msg']['ns']
                index=ns.find('.')
                db_name=ns[:index]
                col_name=ns[index+1:]
                print op,ns,db_name,col_name
                print doc
                #print type(deserialize(body)),deserialize(body)
                ch.basic_ack(delivery_tag = method.delivery_tag)
                print 'be received'
            except KeyError,ValueError:
                logging.info('callback error:operation %d failed') %msg['msg']['h']

        channel.basic_qos(prefetch_count=1) #不要一次给一个worker的信息多于一个
        channel.basic_consume(callback,queue=queue_name)
        print ' [*] Waiting for messages. To exit press CTRL+C'
        channel.start_consuming()



# encoding=utf-8
__author__ = 'bxm'

import logging
import pika
from utils import deserialize, load_yaml
from utils.database import get_mongodb
from core.abstract_class import BaseTrigger


class Trigger(BaseTrigger):
    def __init__(self):
        self.conf_all = load_yaml()
        self.profile=self.conf_all['midware'] if 'midware' in self.conf_all else {}
        self.host = self.profile['server']['host']
        self.port=self.profile['server']['port']
        self.user = self.profile['auth']['user']
        self.password = self.profile['auth']['passwd']



    def check_message(self, message):
        """
        核对该消息是否需要触发MongoDB数据库的更新,返回消息需要触发的数据库集合
        """
        print message
        try:
            if message['msg']['op'] != 'u':
                print 'not update'
                return None
            ns = message['msg']['ns']
        except AttributeError:
            logging.info('message is invalid')
            return None
        db_correspond = self.conf_all['correspond'] if 'correspond' in self.conf_all else {}
        trig_dbs = None
        for key in db_correspond:
            if key == ns:
                trig_dbs = db_correspond[key]
                break
        print 'update', trig_dbs
        return trig_dbs


    def process_message(self):

        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(self.host,
                                       self.port,
                                       '/',
                                       credentials)
        connection = pika.BlockingConnection(parameters=parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange='trigger_exch', type='fanout')
        result = channel.queue_declare(exclusive=True)  # 跟consumer失去联系时删除队列
        queue_name = result.method.queue
        channel.queue_bind(exchange='trigger_exch',
                           queue=queue_name)

        def callback(ch, method, properties, body):
            try:
                msg = deserialize(body)
                # 消息需要触发的数据库集合
                # {'poi.ViewSpot.county': ['_id', 'enName', 'zhName'], 'geo.Locality.county': ['_id', 'code']}
                trig_dbs = self.check_message(msg)
                if trig_dbs:
                    # 根据消息，对相应集合文档进行更新
                    self.update_data(msg, trig_dbs)

                ch.basic_ack(delivery_tag=method.delivery_tag)
                print 'be received'
            except KeyError:
                print 'callback error:operation failed'
                logging.info('callback error:operation failed')

        channel.basic_qos(prefetch_count=1)  # 不要一次给一个worker的信息多于一个
        channel.basic_consume(callback, queue=queue_name)
        print ' [*] Waiting for messages. To exit press CTRL+C'
        channel.start_consuming()

    def update_data(self, msg, trig_dbs):
        """
        根据message，更新与其相对应的数据库
        """
        print trig_dbs
        doc = msg['msg']['o']  # 操作文档
        #ns = msg['msg']['ns']  # 数据库.集合
        for key in trig_dbs:
            tmp = key.split('.')
            db_name = tmp[0]
            collection_name = tmp[1]
            update_dic = {}
            # 如 提取poi.ViewSpot.county中county
            prefix_attr='.'.join(tmp[2:])
            for attr in trig_dbs[key]:
                #内嵌文档
                if prefix_attr:
                    update_attr = prefix_attr+'.'+attr
                else:
                    update_attr=attr
                update_dic[update_attr] = doc[attr]
            col = get_mongodb(db_name, collection_name, 'mongo-raw')
            col.update({prefix_attr + '._id': update_dic[prefix_attr + '._id']}, {'$set': update_dic}, upsert=True)
            # index = ns.find('.')
            #db_name = ns[:index]
            #col_name = ns[index + 1:]
            #print op, ns, db_name, col_name
            #print doc
            #print type(deserialize(body)), deserialize(body)
            #col = get_mongodb()


trigger = Trigger()
trigger.process_message()
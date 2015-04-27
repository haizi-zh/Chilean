# encoding=utf-8
__author__ = 'bxm'

import logging
import pika
from utils.database import get_mongodb
from core.abstract_class import BaseTrigger


class Trigger(BaseTrigger):
    def __init__(self):
        from utils import load_yaml

        self.conf_all = load_yaml()
        profile = self.conf_all['midware'] if 'midware' in self.conf_all else {}
        self.host = profile['server']['host']
        self.port = profile['server']['port']
        self.user = profile['auth']['user']
        self.password = profile['auth']['passwd']
        self.db_correspond = self.conf_all['correspond'] if 'correspond' in self.conf_all else {}

    def check_message(self, message):
        """
        核对该消息是否需要触发MongoDB数据库的更新,返回消息需要触发的数据库集合
        """
        ns = message['msg']['ns'] if message['msg']['op'] == 'u' else None
        tmp_dbs = self.db_correspond[ns] if ns and ns in self.db_correspond else None
        if not tmp_dbs:
            return None

        doc = message['msg']['o']['$set'] if '$set' in message['msg']['o'] else message['msg']['o']
        if '_id' in doc:
            doc.pop('_id')
        op_id = message['msg']['o2']
        trig_dbs = {}

        for key in tmp_dbs:
            # attr_correspond将不包含_id
            attr_correspond = filter(lambda x: True if x in doc.keys() else False, tmp_dbs[key].keys())
            if attr_correspond:
                trig_dbs[key] = {attr_correspond[i]: tmp_dbs[key][attr_correspond[i]] for i in
                                 range(0, len(attr_correspond))}
                trig_dbs[key]['_id']=tmp_dbs[key]['_id']
        print "returnTriger:",trig_dbs
        return trig_dbs



        # print message
        # try:
        # if message['msg']['op'] != 'u':
        # print 'not update operation'
        #         return None
        #     ns = message['msg']['ns']
        # except AttributeError:
        #     logging.info('message is invalid')
        #     return None
        # db_correspond = self.conf_all['correspond'] if 'correspond' in self.conf_all else {}
        # trig_dbs = None
        #
        # return db_correspond[ns] if ns in db_correspond else None
        #
        #
        # for key in db_correspond:
        #     if key == ns:
        #         trig_dbs = db_correspond[key]
        #         break
        #print 'update operation:', trig_dbs
        # return trig_dbs

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
                from utils import deserialize

                msg = deserialize(body)
                # 消息需要触发的数据库集合
                # 如{'poi.ViewSpot.county': ['_id', 'enName', 'zhName'], 'geo.Locality.county': ['_id', 'code']}
                trig_dbs = self.check_message(msg)
                if trig_dbs:
                    # 根据消息，对相应集合文档进行更新
                    self.update_data(msg, trig_dbs)

                ch.basic_ack(delivery_tag=method.delivery_tag)
                # print 'be received'
            except KeyError:
                print 'callback error:operation failed'
                logging.info('callback error:operation failed')

        channel.basic_qos(prefetch_count=1)  # 不要一次给一个worker的信息多于一个
        channel.basic_consume(callback, queue=queue_name)
        print ' [*] Waiting for messages. To exit press CTRL+C'
        channel.start_consuming()

    def update_data(self, message, trig_dbs):
        """
        根据message，更新与其相对应的数据库
        """
        # print trig_dbs
        doc = message['msg']['o']['$set'] if '$set' in message['msg']['o'] else message['msg']['o']
        if '_id' in doc:
            doc.pop('_id')  # 操作文档
        op_id = message['msg']['o2']['_id']
        # ns = msg['msg']['ns']  # 数据库.集合
        for key in trig_dbs:
            db_collection = key.split('.')
            db_name = db_collection[0]
            collection_name = db_collection[1]
            update_id=trig_dbs[key]['_id']
            trig_dbs[key].pop('_id')
            #update_dic = {attr.values()[0]: doc[attr.keys()[0]] for attr in trig_dbs[key]}
            update_dic = {trig_dbs[key][attr]: doc[attr] for attr in trig_dbs[key]}
            print update_id,':', op_id
            print '$set:',update_dic

            # update_id = ''
            # if trig_dbs[key][0].keys()[0] == '_id':
            #     update_id = trig_dbs[key][0].values()[0]
            # else:
            #     for tmp_dic in trig_dbs[key]:
            #         if tmp_dic.keys()[0] == '_id':
            #             update_id = tmp_dic['_id']
            #             break
            col = get_mongodb(db_name, collection_name, 'mongo-raw')
            col.update({update_id: op_id}, {'$set': update_dic}, upsert=True, multi=True)
            # # 如 提取poi.ViewSpot.country中country
            # prefix_attr = '.'.join(tmp[2:])
            #
            # # attr => update_attr
            # func = lambda attr: '%s%s'%(prefix_attr+'.' if prefix_attr else '', attr)
            #
            # update_dic = {func(attr): doc[attr] for attr in trig_dbs[key]}
            #
            #
            #
            # for attr in trig_dbs[key]:
            # update_attr = '%s%s'%(prefix_attr+'.' if prefix_attr else '', attr)
            #     #内嵌文档
            #     if prefix_attr:
            #         update_attr = prefix_attr + '.' + attr
            #     else:
            #         update_attr = attr
            #
            #     update_dic[update_attr] = doc[attr]


            #col.update({prefix_attr + '._id': update_dic[prefix_attr + '._id']}, {'$set': update_dic}, upsert=True)
            # index = ns.find('.')
            #db_name = ns[:index]
            #col_name = ns[index + 1:]
            #print op, ns, db_name, col_name
            #print doc
            #print type(deserialize(body)), deserialize(body)
            #col = get_mongodb()


trigger = Trigger()
trigger.process_message()
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

        # doc = message['msg']['o']['$set'] if '$set' in message['msg']['o'] else message['msg']['o']
        doc_set = {} if '$set' not in message['msg']['o'] else message['msg']['o']['$set']
        doc_unset = {} if '$unset' not in message['msg']['o'] else message['msg']['o']['$unset']
        doc_update = {} if doc_set or doc_unset else message['msg']['o']
        if '_id' in doc_update:
            doc_update.pop('_id')
        trig_dbs = {'update': {}, 'set': {}, 'unset': {}}

        for key in tmp_dbs:
            # attr_correspond将不包含_id
            attr_set = filter(lambda x: True if x in doc_set.keys() else False, tmp_dbs[key].keys())
            attr_unset = filter(lambda x: True if x in doc_unset.keys() else False, tmp_dbs[key].keys())
            attr_update = filter(lambda x: True if x in doc_update.keys() else False, tmp_dbs[key].keys())
            if attr_update:
                trig_dbs['update'][key] = {attr_update[i]: tmp_dbs[key][attr_update[i]] for i in
                                 range(0, len(attr_update))}
                trig_dbs['update'][key]['_id'] = tmp_dbs[key]['_id']
            if attr_set:
                trig_dbs['set'][key] = {attr_set[i]: tmp_dbs[key][attr_set[i]] for i in
                                 range(0, len(attr_set))}
                trig_dbs['set'][key]['_id'] = tmp_dbs[key]['_id']
            if attr_unset:
                trig_dbs['unset'][key] = {attr_unset[i]: tmp_dbs[key][attr_unset[i]] for i in
                                 range(0, len(attr_unset))}
                trig_dbs['unset'][key]['_id'] = tmp_dbs[key]['_id']

        print "returnTriger:", trig_dbs
        return trig_dbs


        # print message
        # try:
        # if message['msg']['op'] != 'u':
        # print 'not update operation'
        # return None
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
        # print 'update operation:', trig_dbs
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
        doc_set = {} if '$set' not in message['msg']['o'] else message['msg']['o']['$set']
        doc_unset = {} if '$unset' not in message['msg']['o'] else message['msg']['o']['$unset']
        doc_update = {} if doc_set or doc_unset else message['msg']['o']
        if '_id' in doc_update:
            doc_update.pop('_id')  # 操作文档
        op_id = message['msg']['o2']['_id']
        # ns = msg['msg']['ns']  # 数据库.集合
        for key in trig_dbs['update']:
            db_collection = key.split('.')
            db_name = db_collection[0]
            collection_name = db_collection[1]
            update_id = trig_dbs['update'][key]['_id']
            trig_dbs['update'][key].pop('_id')
            # update_dic = {attr.values()[0]: doc[attr.keys()[0]] for attr in trig_dbs[key]}
            update_dic = {trig_dbs['update'][key][attr]: doc_update[attr] for attr in trig_dbs['update'][key]}
            print update_id, ':', op_id
            print key, ' update:', update_dic
            col = get_mongodb(db_name, collection_name, 'mongo-raw')
            col.update({update_id: op_id}, {'$set': update_dic}, upsert=True, multi=True)

        for key in trig_dbs['set']:
            db_collection = key.split('.')
            db_name = db_collection[0]
            collection_name = db_collection[1]
            set_id = trig_dbs['set'][key]['_id']
            trig_dbs['set'][key].pop('_id')
            set_dic = {trig_dbs['set'][key][attr]: doc_set[attr] for attr in trig_dbs['set'][key]}
            print set_id, ':', op_id,type(op_id)
            print key, ' $set:', set_dic
            col = get_mongodb(db_name, collection_name, 'mongo-raw')
            col.update({set_id: op_id}, {'$set': set_dic}, upsert=True, multi=True)

        for key in trig_dbs['unset']:
            db_collection = key.split('.')
            db_name = db_collection[0]
            collection_name = db_collection[1]
            unset_id = trig_dbs['unset'][key]['_id']
            trig_dbs['unset'][key].pop('_id')
            unset_dic = {trig_dbs['unset'][key][attr]: doc_unset[attr] for attr in trig_dbs['unset'][key]}
            print unset_id, ':', op_id
            print key, ' $unset:', unset_dic
            col = get_mongodb(db_name, collection_name, 'mongo-raw')
            col.update({unset_id: op_id}, {'$unset': unset_dic}, upsert=True, multi=True)

            # update_id = ''
            # if trig_dbs[key][0].keys()[0] == '_id':
            #     update_id = trig_dbs[key][0].values()[0]
            # else:
            #     for tmp_dic in trig_dbs[key]:
            #         if tmp_dic.keys()[0] == '_id':
            #             update_id = tmp_dic['_id']
            #             break

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
#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'bxm'

from core.abstract_class import BaseProcessor
from utils.database import get_mongodb
from utils import EndProcessException
import pika
import logging

class CorrespondenceProcess(BaseProcessor):

    name = 'correspondence'

    def __init__(self):
        from utils import load_yaml

        self.conf_all = load_yaml()
        self.db_cor = self.conf_all['correspondence'] if 'correspondence' in self.conf_all else {}

    def update(self, message, trig_dbs):
        """
        根据message，更新与其相对应的数据库的字段trig_dbs
        """
        doc_set = {} if '$set' not in message['msg']['o'] else message['msg']['o']['$set']
        doc_unset = {} if '$unset' not in message['msg']['o'] else message['msg']['o']['$unset']
        doc_update = {} if doc_set or doc_unset else message['msg']['o']
        if '_id' in doc_update:
            doc_update.pop('_id')  # 操作文档
        # message中原始被修改文档的_id
        op_id = message['msg']['o2']['_id']

        # 以下三种消息的处理覆盖了所有类型的数据库update

        # 处理普通update,
        for key in trig_dbs['update']:
            db_name, col_name = key.split('.')[:2]
            # db_collection = key.split('.')
            # db_name = db_collection[0]
            # col_name = db_collection[1]
            update_id = trig_dbs['update'][key]['_id']
            trig_dbs['update'][key].pop('_id')
            update_dic = {trig_dbs['update'][key][attr]: doc_update[attr] for attr in trig_dbs['update'][key]}
            # print update_id, ':', op_id
            # print key, ' update:', update_dic
            logging.info('update with %s:%s %s in %s of %s' %(update_id,op_id,update_dic,col_name,db_name))
            col = get_mongodb(db_name, col_name, 'mongo-raw')
            col.update({update_id: op_id}, {'$set': update_dic}, multi=True)

        # 处理$set式的update
        for key in trig_dbs['set']:
            db_name, col_name = key.split('.')[:2]
            # correspondence.yaml中与_id对应的内嵌文档字段
            set_id = trig_dbs['set'][key]['_id']
            trig_dbs['set'][key].pop('_id')
            set_dic = {}
            for attr in trig_dbs['set'][key]:
                if attr in doc_set:
                    set_dic.setdefault(trig_dbs['set'][key][attr], doc_set[attr])
                # 对于doc_set中形如'a.2'内嵌文档字段的处理
                else:
                    attr_t = self.match_list(attr, doc_set)
                    key_tmp = trig_dbs['set'][key][attr] + attr_t[len(attr):]
                    set_dic.setdefault(key_tmp, doc_set[attr_t])

            # print set_id, ':', op_id
            # print key, ' $set:', set_dic
            # print db_name, col_name
            col = get_mongodb(db_name, col_name, 'mongo-raw')
            col.update({set_id: op_id}, {'$set': set_dic}, multi=True)
            # raise EndProcessException

        # 处理$unset式的update
        for key in trig_dbs['unset']:
            db_name, col_name = key.split('.')[:2]
            unset_id = trig_dbs['unset'][key]['_id']
            trig_dbs['unset'][key].pop('_id')
            unset_dic = {}
            for attr in trig_dbs['unset'][key]:
                if attr in doc_unset:
                    unset_dic.setdefault(trig_dbs['unset'][key][attr], doc_unset[attr])
                else:
                    attr_t = self.match_list(attr, doc_unset)
                    key_tmp = trig_dbs['unset'][key][attr] + attr_t[len(attr):]
                    unset_dic.setdefault(key_tmp, doc_unset[attr_t])
            # set_dic = {trig_dbs['unset'][key][attr]: doc_set[attr] for attr in trig_dbs['unset'][key]}
            col = get_mongodb(db_name, col_name, 'mongo-raw')
            col.update({unset_id: op_id}, {'$unset': unset_dic}, multi=True)


    def filter(self, message):
        """
        核对该消息是否需要触发MongoDB数据库的更新,返回消息需要触发的数据库对应字段组成的字典；若无触发返回None
        """

        ns = message['msg']['ns'] if message['msg']['op'] == 'u' else None
        tmp_dbs = self.db_cor[ns] if ns and ns in self.db_cor else None
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

            attr_set = filter(lambda x: True if self.match_list(x, doc_set) else False, tmp_dbs[key].keys())
            # attr_set = filter(lambda x: True if x in doc_set.keys() else False, tmp_dbs[key].keys())
            attr_unset = filter(lambda x: True if self.match_list(x, doc_unset) else False, tmp_dbs[key].keys())
            attr_update = filter(lambda x: True if x in doc_update else False, tmp_dbs[key].keys())
            if attr_update:
                trig_dbs['update'][key] = {attr_update[i]: tmp_dbs[key][attr_update[i]] for i in
                                           range(0, len(attr_update))}
                trig_dbs['update'][key]['_id'] = tmp_dbs[key]['_id']
            if attr_set:
                trig_dbs['set'][key] = {attr_set[i]: tmp_dbs[key][attr_set[i]] for i in range(0, len(attr_set))}
                trig_dbs['set'][key]['_id'] = tmp_dbs[key]['_id']
            if attr_unset:
                trig_dbs['unset'][key] = {attr_unset[i]: tmp_dbs[key][attr_unset[i]] for i in
                                          range(0, len(attr_unset))}
                trig_dbs['unset'][key]['_id'] = tmp_dbs[key]['_id']

        return trig_dbs

    def match_list(self, x, keys_list):
        """
        在keys_list列表里匹配x,如match_list('a',['a.2',3,4])返回'a.2'
        """
        if x in keys_list:
            return x
        for element in keys_list:
            if len(x) < len(element) and x + '.' == element[0:len(x) + 1]:
                return element
        return None


#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
from utils.database import get_mongodb
from core.abstract_class import BaseProcessor

class PoiProcess(BaseProcessor):
    name = 'poi'
    """
    监督poi(db)是否有删除操作
    """
    def __init__(self):
        pass

    def update(self, msg):
        self.filter(msg)

    def filter(self, msg):
        pass
        # op = msg.get('op', None)
        # ns = msg.get('ns', None)
        # print msg
        # if ns is None:
        #     return
        # db, col = ns.split('.')
        # print ns
        # conn = get_mongodb('user', 'Favorite', 'mongo')
        # # compare db name
        # if 'poi' == db:
        #     o = msg.get('o', None)
        #     mid = o.get('_id', None)
        #     print mid
        #     if 'd' == op and mid is not None:
        #         # conn.remove({'_id': mid})
        #         pass
        #     if 'u' == op and mid is not None:
        #         if mid == '5492d06fe721e7171745e160':
        #             print '=======success  ======='
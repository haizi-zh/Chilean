#!/usr/bin/env python
# -*- coding: utf-8 -*-


from core.abstract_class import BaseProcessor

class Viewspot(BaseProcessor):
    name = 'viewspot'
    def __init__(self):
        pass

    def update(self, msg):
        self.process(msg)

    def process(self, msg):
        pass
        # print self.name + 'confirm'

    def filter(self,msg):
        pass


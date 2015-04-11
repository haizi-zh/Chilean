#!/usr/bin/env python
# -*- coding: utf-8 -*-


from core.abstract_class import BaseProcessor


class Example(BaseProcessor):
    name = 'example'
    def __init__(self):
        pass

    def update(self, msg):
        """
        implemet this func
        :param msg:
        :return:
        """
        self.process(msg)

    def process(self, msg):
        pass






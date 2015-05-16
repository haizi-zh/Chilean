# coding=utf-8
__author__ = 'bxm'

from utils import load_yaml, global_conf
import os
import imp
import sys
import pika
import logging
from core.abstract_class import BaseProcessor


def reg_processor(processor_dir=None):
    """
    将processor路径下的processor类进行注册
    """
    if not processor_dir:
        root_dir = os.path.normpath(os.path.split(__file__)[0])
        processor_dir = os.path.normpath(os.path.join(root_dir, 'processor'))
        global_conf['processor'] = {}

    for cur, d_list, f_list in os.walk(processor_dir):
        # 获得包路径
        package_path = []
        tmp = cur
        while True:
            d1, d2 = os.path.split(tmp)
            package_path.insert(0, d2)
            if d2 == 'processor' or d1 == '/' or not d1:
                break
            tmp = d1
        package_path = '.'.join(package_path)
        for f in f_list:
            f = os.path.normpath(os.path.join(cur, f))
            tmp, ext = os.path.splitext(f)
            if ext != '.py':
                continue
            p, fname = os.path.split(tmp)

            try:
                ret = imp.find_module(fname, [p]) if p else imp.find_module(fname)
                mod = imp.load_module(fname, *ret)

                for attr_name in dir(mod):
                    try:
                        c = getattr(mod, attr_name)
                        if issubclass(c, BaseProcessor) and c != BaseProcessor:
                            name = getattr(c, 'name')
                            if name:
                                global_conf['processor'][name] = c
                    except TypeError:
                        pass
            except ImportError:
                print 'Import error: %s' % fname
                raise


def start_process():
    # if processor_name in global_conf:
    #     processor_class = global_conf['processor'][processor_name]
    from utils import load_yaml

    conf_all = load_yaml()
    profile = conf_all['rbmq'] if 'rbmq' in conf_all else {}
    host = profile['server']['host']
    port = profile['server']['port']
    user = profile['auth']['user']
    password = profile['auth']['passwd']
    db_correspond = conf_all['correspond'] if 'correspond' in conf_all else {}
    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(host,
                                            port,
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
            for processor_name in global_conf['processor']:
                processor_class = global_conf['processor'][processor_name]
                processor_tmp=processor_class()
                tri_tmp = processor_tmp.filter(msg)
                if tri_tmp:
                    processor_tmp.update(msg,tri_tmp)
                # 消息需要触发的数据库集合
                # 如{'poi.ViewSpot.county': ['_id', 'enName', 'zhName'], 'geo.Locality.county': ['_id', 'code']}

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except KeyError:
            print 'callback error:operation failed! Message: %s' % body
            logging.info('callback error:operation failed! Message: %s' % body)

    channel.basic_qos(prefetch_count=1)  # 不要一次给一个worker的信息多于一个
    channel.basic_consume(callback, queue=queue_name)
    channel.start_consuming()

def main():
    # import argparse
    # parser = argparse.ArgumentParser()
    # parser.add_argument('cmd', type=str)
    # parser.add_argument('--logpath', type=str)
    # parser.add_argument('--log2file', action='store_true')
    # args, leftovers = parser.parse_known_args()
    #
    # print sys.argv
    # msg = 'PROCESSOR STARTED: %s' % ' '.join(sys.argv)
    # print 'msg',msg
    start_process()



if __name__ == "__main__":
    old_dir = os.getcwd()
    os.chdir(os.path.normpath(os.path.split(__file__)[0]))

    reg_processor()
    main()

    os.chdir(old_dir)

#!/usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'zephyre'
import types
from bson.json_util import dumps
from bson.json_util import loads


global_conf = {}

def load_yaml():
    """
    Load YAML-format configuration files
    :return:
    """

    config = getattr(load_yaml, 'config', None)
    if config:
        return config

    from yaml import load
    import os
    from glob import glob

    cfg_dir = os.path.abspath(os.path.join(os.path.split(__file__)[0], '../conf/'))
    cfg_file = os.path.join(cfg_dir, 'Chilean.yaml')
    with open(cfg_file) as f:
        config = load(f)

    # Resolve includes
    if 'include' in config:
        for entry in config['include']:
            for fname in glob(os.path.join(cfg_dir, entry)):
                if fname == cfg_file:
                    continue
                try:
                    with open(fname) as f:
                        include_data = load(f)
                        for k, v in include_data.items():
                            config[k] = v
                except IOError:
                    continue

    setattr(load_yaml, 'config', config)
    return config


def load_config():
    """
    Load configuration files from ./conf/*.cfg
    """

    conf = getattr(load_config, 'conf', {})

    if conf:
        return conf
    else:
        import ConfigParser
        import os

        root_dir = os.path.normpath(os.path.split(__file__)[0])
        cfg_dir = os.path.normpath(os.path.join(root_dir, '../conf'))
        it = os.walk(cfg_dir)
        cf = ConfigParser.ConfigParser()
        for f in it.next()[2]:
            if os.path.splitext(f)[-1] != '.cfg':
                continue
            cf.read(os.path.normpath(os.path.join(cfg_dir, f)))

            for s in cf.sections():
                section = {}
                for opt in cf.options(s):
                    section[opt] = cf.get(s, opt)
                conf[s] = section

        setattr(load_config, 'conf', conf)
        return conf


def mercator2wgs(mx, my):
    """
    墨卡托坐标向WGS84坐标的转换
    :param mx:
    :param my:
    :return:
    """
    from math import pi, atan, exp

    x = mx / 20037508.34 * 180
    y = my / 20037508.34 * 180
    y = 180 / pi * (2 * atan(exp(y * pi / 180)) - pi / 2)
    return x, y


def guess_coords(x, y):
    # 可能是墨卡托
    if abs(x) > 180 or abs(y) > 180:
        rx, ry = mercator2wgs(x, y)
    else:
        rx, ry = x, y

    if abs(x) < 0.1 and abs(y) < 0.1:
        # 不考虑在原点的情况
        return

    if abs(ry) >= 90:
        rx, ry = ry, rx
    if abs(ry) >= 90:
        return

    return rx, ry


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    from math import radians, sin, cos, asin, sqrt
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))

    # 6367 km is the radius of the Earth
    km = 6367 * c
    return km


def serialize(my_dict):
    if not isinstance(my_dict, types.DictType):
        raise TypeError
    return dumps(my_dict)


def deserialize(obj_str):

    if not isinstance(obj_str, types.StringType):
        raise TypeError
    try:
        return loads(obj_str)
    except ValueError:
        print 'input string can\'t be deserialized'

class EndProcessException(Exception):
  pass
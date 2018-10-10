#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018-9-27 下午3:04
# @Author  : Jeffrey
# @Site    : 
# @File    : six.py
# @Software: PyCharm
"""
功能：
"""

import datetime
import json


class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


def show_json(_dict):
    return json.dumps(_dict, indent=2,cls=CJsonEncoder)


def find_last_aggregation(tree):
    if "aggs" in tree:
        return find_last_aggregation(tree["aggs"])
    else:
        for kw, value in tree.iteritems():
            if kw.split("__")[-1] == "group":
                return find_last_aggregation(value["aggs"])
        return tree


def concat_aggregate_name(*name):
    return "__".join(name)


def dict2tuples(_dict):
    return tuple((k, v) for k, v in _dict.iteritems())



if __name__ == "__main__":
    print dict2tuples({"1": 2, "3": 5})

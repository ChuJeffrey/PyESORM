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

import json


def show_json(_dict):
    return json.dumps(_dict, indent=2)


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

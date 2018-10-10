#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018-8-28 上午10:24
# @Author  : Jeffrey
# @Site    : 
# @File    : exceptions.py
# @Software: PyCharm
"""
功能：
"""


# import exceptions as pyexception


class ValidationError(Exception):
    def __init__(self, error_message=None, code='invalid', params=None):
        if code == "invalid":
            if isinstance(params, dict):
                for k, v in params.iteritems():
                    print "(%s: %s) %s" % (k, v, error_message)


class InterruptError(object):
    def __init__(self, error_messages=None):
        print "%s" % error_messages
        raise


class MissWarning(object):
    def __init__(self, error_messages=None):
        print "%s" % error_messages

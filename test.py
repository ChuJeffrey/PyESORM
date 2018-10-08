#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018-10-8 下午3:43
# @Author  : Jeffrey
# @Site    : 
# @File    : test.py
# @Software: PyCharm
"""
功能：
"""

from models import *


class TestModel(Model):
    class Meta:
        doc_type = "test"

    label = JsonField(default=[])
    title = KeywordField(default="")


if __name__ == "__main__":
    t = TestModel.objects.filter(label={"industry": "手机"}, title="新")
    for _t in t:
        print _t

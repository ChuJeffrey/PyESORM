#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018-8-27 下午5:14
# @Author  : Jeffrey
# @Site    : 
# @File    : configs.py
# @Software: PyCharm
"""
功能： 引入配置或者采取默认配置
"""

from config import settings

ES_HOSTS = [{"host": "172.16.6.102"}, ]
ES_HTTP_AUTH = ('', '')


ES_CONFIG = {
    "default": {
        "hosts": ES_HOSTS,
        "auth": ES_HTTP_AUTH,
        "alias_name": settings.PROJECT_TYPE+"_videos_alias"
    },
    "mihui_video": {
        "hosts": ES_HOSTS,
        "auth": ES_HTTP_AUTH,
        "alias_name": settings.PROJECT_TYPE+"_videos_alias"
    },
    "mihui_weibo": {
        "hosts": ES_HOSTS,
        "auth": ES_HTTP_AUTH,
        "alias_name": settings.PROJECT_TYPE+"_weibo_feed_alias"
    },
    "mihui_wechat": {
        "hosts": ES_HOSTS,
        "auth": ES_HTTP_AUTH,
        "alias_name": settings.PROJECT_TYPE+"_wechat_feed_alias"
    }
}

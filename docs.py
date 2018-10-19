#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018-8-30 上午11:25
# @Author  : Jeffrey
# @Site    : 
# @File    : doct.py
# @Software: PyCharm
"""
功能：
"""

from inner_library.py_es_orm.core import C, RangeBy, Max, Min, Avg
from inner_library.py_es_orm import models


class VideoFeedESModel(models.Model):
    class Meta:
        pass

    video_source = models.KeywordField(max_length=20, default="")
    video_id = models.KeywordField(max_length=100, default="")
    forward_count = models.LongField(default=0)
    like_count = models.LongField(default=0)
    comment_count = models.LongField(default=0)
    play_count = models.LongField(default=0)
    hot = models.LongField(default=0)
    publisher_name = models.TextField(default="")
    publisher_id = models.KeywordField(default="")
    publisher_avatar_url = models.TextField(default="")
    publisher_name_split = models.TextField(default="")
    category = models.KeywordField(default="")
    challenge_id = models.KeywordField(default="")
    create_time = models.DateTimeField(help_text="源平台发布时间")
    thumbnail_url = models.TextField(default="")
    video_title = models.TextField(default="")
    video_title_split = models.TextField(default="")
    video_voice_subtitle = models.TextField(default="")
    video_voice_subtitle_split = models.TextField(default="")

    objects = models.Manager(es_conn_name="mihui_video", doc_type="video_search")


if __name__ == "__main__":
    # range_by
    vq = VideoFeedESModel.objects.filter(hot__gte=0).range_by(
        "create_time", *[("2018-03-01", "2018-04-01"), ("2018-03-01", "2018-04-01")]).aggregate(hm=Max("hot"))
    print vq.groups()


    # group_by
    t = VideoFeedESModel.objects.filter(
        hot__gt=0). \
        group_by("publisher_id", "like_count"). \
        aggregate(hm=Min("hot"),
                  pm=Max("play_count"),
                  vc=Avg("forward_count"))
    print t.groups("69228077034", 34)
    # print t.json()
    for _t in t:
        print _t

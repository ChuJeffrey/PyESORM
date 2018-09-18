#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018-8-27 下午5:01
# @Author  : Jeffrey
# @Site    :
# @File    : base.py
# @Software: PyCharm
"""
功能： 核心
"""

import json
import copy
import elasticsearch

from es_exceptions import InterruptError

from configs import ES_CONFIG

default_conn = elasticsearch.Elasticsearch(
    sniff_on_start=True,
    sniff_on_connection_fail=True,
    sniffer_timeout=600,
    timeout=60, **(ES_CONFIG.get("default"))
)


def get_es_default_params():
    return ES_CONFIG.get("default")["alias_name"]


class MatchType(object):
    MATCH_PHRASE = "match_phrase"
    MATCH = "match"
    MATCH_ALL = "match_all"
    DEFAULT = MATCH_PHRASE


class FilterType(object):
    LTE = "lte"
    GTE = "gte"
    LT = "lt"
    GT = "gt"
    RANGE = "range"
    IN = "in"


class MatchOperator(object):
    MUST = "must"
    SHOULD = "should"
    MUST_NOT = "must_not"
    DEFAULT = MUST


class ClsMgrMap(object):
    data = {

    }

    @classmethod
    def add_map(cls, doc_type, model_class, manager):
        cls.data[doc_type] = [model_class, manager]

    @classmethod
    def get_map_by_doc_type(cls, doc_type):
        return cls.data.get(doc_type) or None, None


class MatchNode(object):
    """最小条件"""
    default = MatchOperator.DEFAULT

    def __init__(self, children=None, operator=None, negated=False):

        self.children = children[:] if children else []
        self.operator = operator or self.default
        self.negated = negated

    @classmethod
    def _new_instance(cls, children=None, operator=None, negated=False):
        obj = MatchNode(children, operator, negated)
        obj.__class__ = cls
        return obj

    def __str__(self):
        if self.negated:
            return '(must_not (%s: %s))' % (
                self.operator, ', '.join(str(c) for c in self.children))
        return '(%s: %s)' % (self.operator, ', '.join(str(c) for c in self.children))

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self)

    def __contains__(self, other):
        """
        Returns True is 'other' is a direct child of this instance.
        """
        return other in self.children

    def __len__(self):
        """
        The size of a node if the number of children it has.
        """
        return len(self.children)

    def add(self, node, operator, squash=True):
        if node in self.children:
            return node
        if not squash:
            self.children.append(node)
            return node
        if self.operator == operator:  # 运算符与当前节点的一致时

            # 将与当前节点(self)同类型(同must或者同should)的、且不带must_not的单例集合进行横向合并进来
            if (isinstance(node, MatchNode) and not node.negated and
                    (node.operator == operator or len(node) == 1)):
                self.children.extend(node.children)
                return self
            else:
                # 将复杂集合进行标记，用后面的逻辑来遍历还原
                self.children.append(node)
                return node
        else:  # 运算符与当前节点的不一致时, 则将自己变成儿子，自己改成新的运算符
            obj = self._new_instance(self.children, self.operator, self.negated)
            self.operator = operator
            self.children = [obj, node]
            return node

    def negate(self):
        """
        Negate the sense of the root connector.
        """
        self.negated = not self.negated


class C(MatchNode):
    def __init__(self, *args, **kwargs):
        super(C, self).__init__(children=list(args) + list(kwargs.iteritems()))

    def _combine(self, otherC, operator):
        if not isinstance(otherC, C):
            InterruptError("otherC is not a C")
        obj = type(self)()
        obj.operator = operator
        obj.add(self, operator)
        obj.add(otherC, operator)
        return obj

    def __or__(self, other):
        return self._combine(other, MatchOperator.SHOULD)

    def __and__(self, other):
        return self._combine(other, MatchOperator.MUST)

    def __invert__(self):
        obj = type(self)()
        obj.add(self, MatchOperator.MUST)
        obj.negate()
        return obj

    # def _clone(self, ):
    #     return copy.deepcopy(self)

    def _clone(self):
        clone = self.__class__._new_instance(
            children=[], operator=self.operator, negated=self.negated)
        for child in self.children:  # 全面克隆
            if hasattr(child, 'clone'):
                clone.children.append(child.clone())
            else:
                clone.children.append(child)
        return clone

    def json(self):
        return json.dumps(self.transform(self), indent=4)

    @staticmethod
    def transform(data):
        query = {
        }
        if isinstance(data, tuple) or isinstance(data, C):
            if isinstance(data, tuple):
                return C.pack_match(data)
            else:  # C object
                bool_query = {}
                for child in data.children:
                    child_result = C.transform(child)
                    if child_result:
                        if data.operator in bool_query:
                            bool_query[data.operator].append(child_result)
                        else:
                            bool_query[data.operator] = [child_result]
                if data.negated:
                    query["bool"] = {
                        MatchOperator.MUST_NOT: {
                            "bool": bool_query
                        }
                        if len(bool_query) != 1 else bool_query.items()[0][-1]  # todo 将多余的子集合移位
                    }
                else:
                    query["bool"] = bool_query
                if MatchOperator.SHOULD in query["bool"]:
                    query["bool"]["minimum_should_match"] = 1
        return query

    @staticmethod
    def pack_match(data):
        """
        打包转换成可用的es dsl
        :param data: tuple: (k, v)
        :return:
        """
        key, value = data
        match_type, query = MatchType.MATCH_PHRASE, dict([data])
        if "__" in key:
            field, filter_type = "".join(key.split("__")[:-1]), key.split("__")[-1]
            match_type = filter_type
            if filter_type == MatchType.MATCH:
                query = dict([(field, value)])
                return {match_type: query}
            elif filter_type in [FilterType.LTE, FilterType.LT, FilterType.GTE, FilterType.GT]:
                query = {
                    "range": {
                        field: {
                            filter_type: value,
                        }
                    }
                }
                return query
            elif filter_type in [FilterType.IN]:
                pass
        else:
            return {
                match_type: query
            }


class DocQuerySet(object):
    def __init__(self, model_class=None, es_conn=None, doc_type=None, condition=None, query=None, sort_list=None):
        self.model_class = model_class or ClsMgrMap.get_map_by_doc_type(doc_type)

        self.total = 0
        self.condition = condition
        self._es_conn = es_conn or default_conn
        self.args = ()
        self.kwargs = {}
        self.result = []
        self.result_field_names = []
        self.doc_type = doc_type

        self._alias_name = get_es_default_params()
        self.dsl = {}
        if query:
            self.query = query
        else:
            self._init_query()
        self.size = 10
        self._from = 0
        self.sort = sort_list or []
        self.aggs = {}

    def construct_condition(self, condition):
        """
        构造查询条件
        :param condition:
        :return:
        """
        self.condition = condition
        return self.condition

    def update_model_class(self, model_class):
        self.model_class = model_class

    def filter(self, *args, **kwargs):
        """
        主此处filter
        :param args:
        :param kwargs:
        :return:
        """
        return self._filter_or_exclude(False, *args, **kwargs)

    def _filter_or_exclude(self, negated=False, *args, **kwargs):
        if negated:
            self.add_c(~C(*args, **kwargs))
        else:
            self.add_c(C(*args, **kwargs))
        return self._clone()

    def exclude(self, *args, **kwargs):
        return self._filter_or_exclude(True, *args, **kwargs)

    def _init_query(self):
        self.query = {"match_all": {}}

    def remove_match_all(self):
        self.query.pop("match_all")

    def result(self):
        return self.condition

    def total(self):
        pass

    def json(self):
        return self.condition.json()

    def _clone(self, ):
        return self

    def clone(self):
        return self._clone()

    def add_c(self, c_object):
        if self.condition:
            self.condition.add(c_object, operator=MatchOperator.DEFAULT)
        else:
            self.condition = c_object

    def __iter__(self):
        self.fetch_result()
        return iter(self.result)

    def fetch_result(self):
        condition = self.complete_condition()
        # print json.dumps(condition, indent=4)
        results = self._es_conn.search(index=self._alias_name, doc_type=self.doc_type, body=condition)
        for doc in results["hits"]["hits"]:
            if self.result_field_names:
                doc_dict = {}
                for field_name in self.result_field_names:
                    doc_dict[field_name] = doc["_source"].get("field_name")
                self.result.append(doc_dict)
            else:

                class_instance = self.model_class()
                for field_model in self.model_class.Meta.concrete_fields:
                    try:
                        setattr(class_instance, field_model.attname, doc["_source"][field_model.attname])
                    except KeyError:
                        InterruptError("%s attribution is not exist in document" % (field_model.attname, ))
                self.result.append(class_instance)

        # self.result = results["hits"]["hits"]
        self.total = results["hits"]["total"]

    def count(self):
        return self.total

    def values(self, *field_names):
        obj = self._clone()
        obj.result_field_names = field_names
        return obj

    def complete_condition(self):
        self.dsl = {
            "sort": self.sort,
            "query": self.condition.transform(self.condition),
            "from": self._from,
            "size": self.size,
        }
        return self.dsl

    def order_by(self, *field_names):
        """
        排序
        :param field_names: 排序字段列表
        :return:
        """
        obj = self._clone()
        if "sort" in obj.condition:
            obj.condition.pop("sort")
        obj.sort = [
            {field_name[1:]: {"order": "desc"}} if field_name[0] == "-" else {field_name: {"order": "asc"}}
            for field_name in field_names
            ]
        if "score" not in field_names or "-score" not in field_names:
            obj.sort.append({"_score": {"order": "desc"}})

        return obj

    def __getitem__(self, item):
        if isinstance(item, int):
            if not self.result:
                self.fetch_result()
            return self.result[item]
        elif isinstance(item, slice):
            self._from = item.start if item.start else 0
            self.size = item.stop if item.stop else 0
            if not self.result:
                self.fetch_result()
            return self.result


if __name__ == "__main__":
    con = DocQuerySet().filter(~(C(a__gt=1) | ~C(b=2, e=5))).filter(c=3, d=4).exclude(C(f=6) | C(g=7))
    print con.complete_condition()

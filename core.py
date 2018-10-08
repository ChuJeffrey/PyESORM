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

import collections
import copy
import elasticsearch

from es_exceptions import InterruptError

from configs import ES_CONFIG
from six import *

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
    RANGE = "range"


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
        return show_json(self.transform(self))

    @staticmethod
    def transform(data):
        query = {
        }
        if isinstance(data, tuple) or isinstance(data, C):
            if isinstance(data, tuple):
                return C.pack_match(*data)
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
    def create_nested_query(value):
        return

    @staticmethod
    def pack_match(*data_tuple):
        """
        打包转换成可用的es dsl
        :param data: tuple: (k, v)
        :return:
        """
        key, data = data_tuple
        match_type, query = MatchType.MATCH_PHRASE, dict([(key, data)])
        if isinstance(data, dict):
            query = {
                "nested": {
                    "path": key,
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match_phrase": {
                                        key + "." + k: {
                                            "query": value
                                        }
                                    }
                                }
                                for k, value in data.iteritems()
                                ]
                        }
                    }
                }
            }
            return query

        if "__" in key:
            field, filter_type = "".join(key.split("__")[:-1]), key.split("__")[-1]
            match_type = filter_type
            if filter_type == MatchType.MATCH:
                query = dict([(field, data)])
                return {match_type: query}
            elif filter_type in [FilterType.LTE, FilterType.LT, FilterType.GTE, FilterType.GT]:
                query = {
                    "range": {
                        field: {
                            filter_type: data,
                        }
                    }
                }
                return query
            elif filter_type in [FilterType.IN]:
                query = {
                    "terms": {
                        field: data
                    }
                }
                return query
            elif filter_type in [FilterType.RANGE]:
                start, end = list(data)
                query = {
                    "range": {
                        field: {
                            "gte": start,
                            "lte": end
                        }
                    }
                }
                return query
        else:
            return {
                match_type: query
            }


class Aggregate(object):
    AGGREGATION_NAME = "aggs"

    def __init__(self, field_name, *args, **kwargs):
        self.field_name = field_name
        self._dsl = {}
        self.aggregation_field = concat_aggregate_name(self.field_name, self.AGGREGATION_NAME)
        super(Aggregate, self).__init__()

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self)

    def __str__(self):
        return '(%s: %s)' % (self.AGGREGATION_NAME, self.dsl)

    @property
    def dsl(self):
        self._dsl = {
            self.aggregation_field: {
                self.AGGREGATION_NAME: {
                    "field": self.field_name
                }
            }
        }
        return self._dsl

    @dsl.setter
    def dsl(self, dsl):
        self._dsl = dsl


class Sum(Aggregate):
    AGGREGATION_NAME = "sum"


class Max(Aggregate):
    AGGREGATION_NAME = "max"


class Min(Aggregate):
    AGGREGATION_NAME = "min"


class Avg(Aggregate):
    AGGREGATION_NAME = "avg"


class Uni(Aggregate):
    AGGREGATION_NAME = "cardinality"


class GroupBy(Aggregate):
    AGGREGATION_NAME = "group"

    @property
    def dsl(self):
        if not self._dsl:
            self._dsl = {
                self.aggregation_field: {
                    "terms": {
                        "field": self.field_name,
                        "size": 100
                    }
                }
            }
        return self._dsl

    @dsl.setter
    def dsl(self, dsl):
        self._dsl = dsl


class DocQuerySet(object):
    def __init__(self, model_class=None, es_conn=None, doc_type=None, condition=None, query=None, sort_list=None):
        self.model_class = model_class or ClsMgrMap.get_map_by_doc_type(doc_type)
        self.total = 0
        self.condition = condition
        self._es_conn = es_conn or default_conn
        self.args = ()
        self.attrs = {}
        self.dsl = {}
        self.kwargs = {}
        self.result = []
        self.result_field_names = []
        self.doc_type = doc_type

        self._alias_name = get_es_default_params()
        self.query = query
        self.size = 10
        self._from = 0
        self.sort = sort_list or []
        self.aggs_dsl = {}  # dsl
        self.aggregation_list = []
        self.group_by_list = []
        self.group_by_result = {}

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
        clone = self._clone()
        if negated:
            clone.add_c(~C(*args, **kwargs))
        else:
            clone.add_c(C(*args, **kwargs))
        return clone

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
        return show_json(self.complete_condition())

    def _clone(self):
        klass = self.__class__
        instance = klass(self.model_class, es_conn=self._es_conn, doc_type=self.doc_type,
                         condition=copy.deepcopy(self.condition), query=copy.deepcopy(self.query),
                         sort_list=self.sort[:])
        instance.size = self.size
        instance._from = self._from
        instance.dsl = copy.deepcopy(self.dsl)
        instance.aggs_dsl = copy.deepcopy(self.aggs_dsl)
        instance.aggregation_list = self.aggregation_list[:]
        instance.group_by_list = copy.deepcopy(self.group_by_list)
        return instance

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
        if self.result:
            return self.result
        condition = self.complete_condition()
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
                        InterruptError("%s attribution is not exist in document" % (field_model.attname,))
                self.result.append(class_instance)

        self.total = results["hits"]["total"]
        if self.aggs_dsl:
            self.set_aggregation_value(results["aggregations"])

    def _set_group_by_result(self, group_results, output_result):
        for group_key, buckets in group_results.iteritems():
            if group_key.split("__")[-1] == "group":
                for inner_group_results in group_results[group_key]["buckets"]:
                    if not isinstance(output_result, dict):
                        output_result = dict()
                    output_result[inner_group_results["key"]] = dict()
                    self._set_group_by_result(inner_group_results, output_result[inner_group_results["key"]])
            else:
                if not isinstance(output_result, dict):
                    output_result = dict()
                for aggregation in self.aggregation_list:
                    if isinstance(aggregation, Aggregate):
                        field = aggregation.aggregation_field
                        if field in group_results:
                            value = group_results[field]["value"]
                        else:
                            value = None
                        output_result[field] = value

    def set_aggregation_value(self, aggregations_results):
        if self.group_by_list:
            self._set_group_by_result(aggregations_results, self.group_by_result)
        else:
            for aggregation in self.aggregation_list:
                if isinstance(aggregation, Aggregate):
                    field = aggregation.aggregation_field
                    if field in aggregations_results:
                        value = aggregations_results[field]["value"]
                    else:
                        value = None
                    if len(self.aggregation_list) == 1:
                        self.attrs[self.aggregation_list[0].aggregation_field] = value
                    self.attrs[field] = value

    def count(self):
        return self.total

    def values(self, *field_names):
        obj = self._clone()
        obj.result_field_names = field_names
        return obj

    def complete_condition(self):
        _l = [
            ("sort", self.sort),
            ("query", self.condition.transform(self.condition)),
            ("aggs", self.aggs_dsl),
            ("from", self._from),
            ("size", self.size),
        ]
        self.dsl = collections.OrderedDict(_l)
        return self.dsl

    def order_by(self, *field_names):
        """
        排序
        :param field_names: 排序字段列表, 有“-”表示降序排序，无则升序排序
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
        elif isinstance(item, str):
            if not self.result:
                self.fetch_result()
            return self.attrs.get(item)

    def aggregate(self, *aggs, **kwargs):
        # self.aggregation_list.extend(aggs)
        """
        {"hot_sum": {
            "sum": {
                "field": "hot"
                }
            }
        }
        """

        # self.aggregation_list.extend(dict2tuples(kwargs))
        """
        (hot,
        {"hot_sum": {
            "sum": {
                "field": "hot"
                }
            }
        },)
        """
        obj = self._clone()
        query_aggs = {}
        for agg in aggs:
            if isinstance(agg, Aggregate):
                query_aggs.update(agg.dsl)
                obj.aggregation_list.append(agg)
        for new_field_name, sub_aggregate in kwargs.iteritems():
            if isinstance(sub_aggregate, Aggregate):
                # 重置字段别名
                sub_aggregate.aggregation_field = new_field_name
                query_aggs.update(sub_aggregate.dsl)
                obj.aggregation_list.append(sub_aggregate)
        _inner_aggs = find_last_aggregation(obj.aggs_dsl)
        _inner_aggs.update(query_aggs)
        return obj

    def group_by(self, *field_names):
        obj = self._clone()
        for field_name in field_names:
            g = GroupBy(field_name)
            _inner_aggs = find_last_aggregation(obj.aggs_dsl)
            copy_inner_aggs = copy.deepcopy(_inner_aggs)
            _inner_aggs.clear()
            g_dsl = copy.deepcopy(g.dsl)
            g_dsl[g.aggregation_field]["aggs"] = copy_inner_aggs
            g.dsl = g_dsl
            _inner_aggs.update(g.dsl)
            obj.group_by_list.append(g)
        return obj

    def groups(self, *args):
        self.fetch_result()
        _result = self.group_by_result
        for key in args:
            _result = _result[key]
        return _result


if __name__ == "__main__":
    con = DocQuerySet().filter(~(C(a__gt=1) | ~C(b=2, e=5))).filter(c=3, d=4).exclude(C(f=6) | C(g=7))
    print con.json()

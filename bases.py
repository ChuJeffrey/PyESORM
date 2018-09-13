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

from configs import ES_HOSTS

es_connection = elasticsearch.Elasticsearch(
    ES_HOSTS,
    sniff_on_start=True,
    sniff_on_connection_fail=True,
    sniffer_timeout=600,
    timeout=60,
)


class BaseManager(object):
    """
    连接池
    """
    pass


class Compiler(object):
    es_conn = es_connection

    def execute_dsl(self, index_name, doc_type_list, condition, params):
        results = self.es_conn.search(index=index_name, doc_type=self.to_doc_type_str(doc_type_list),
                                      body=condition, params=params)
        return results

    @staticmethod
    def to_doc_type_str(doc_type_list):
        try:
            return "/".join(doc_type_list)
        except:
            return "*"


class MatchType(object):
    MATCH_PHRASE = "match_phrase"
    MATCH = "match"
    MATCH_ALL = "match_all"


class MatchOperator(object):
    MUST = "must"
    SHOULD = "should"
    MUST_NOT = "must_not"


class MatchNode(object):
    """最小条件"""
    BASE_NODE = "leaf"
    NODE = "node"

    def __init__(self, node_type=None, operator=None, children=None, match_type=None, negated=False):
        self.node_type = node_type or MatchNode.BASE_NODE
        self.match_type = match_type or MatchType.MATCH_PHRASE
        self.operator = operator
        self.children = children[:] if children else []
        self.negated = negated

    def transform(self):
        """
        node = {
            "operator": "must",
            "match_type": "match_phrase",
            "node_type": "node",
            "children": [
                {
                    "operator": "must",
                    "node_type": "leaf",
                    "match_type": "match_phrase",
                    "children": [
                        {'a': 0},
                        {'b': 200}
                    ]
                },
                {
                    "operator": "must",
                    "node_type": "leaf",
                    "match_type": "match",
                    "children": [
                        {'c': "韩后"},
                        {'d': "xyz"}
                    ]
                }
            ]
        }
        """
        tree_data = {
        }
        if self.operator not in tree_data:
            tree_data[self.operator] = []
        if self.node_type == MatchNode.BASE_NODE:
            tree_data[self.operator].extend(
                [{self.match_type: data} for data in self.children])
        else:
            tree_data[self.operator].extend(
                [{"bool": node.transform()} for node in self.children])
        return tree_data

    def add(self, node, operator):
        if self.node_type != MatchNode.BASE_NODE:
            if operator == MatchOperator.SHOULD:
                # self.children.extend(node.children)
                obj = self._new_instance(self.node_type, self.operator, self.children,
                                         self.match_type, self.negated)
                self.operator = operator
                self.children = [obj, node]
                self.node_type = MatchNode.NODE
            else:
                self.children.append(node)
        else:  # 叶子变成父节点
            obj = self._new_instance(self.node_type, self.operator, self.children,
                                     self.match_type, self.negated)
            self.operator = operator
            self.children = [obj, node]
            self.node_type = MatchNode.NODE

    @classmethod
    def _new_instance(cls, node_type=None, operator=None, children=None,
                      match_type=MatchType.MATCH_PHRASE, negated=False):
        obj = MatchNode(node_type, operator, children, match_type, negated)
        obj.__class__ = cls
        return obj


class C(MatchNode):
    default_operator = MatchOperator.MUST

    def __init__(self, *args, **kwargs):
        operator = kwargs.get("operator") or MatchOperator.MUST
        match_type = kwargs.get("match_type") or MatchType.MATCH_PHRASE
        node_type = kwargs.get("node_type") or MatchNode.NODE
        negated = kwargs.get("negated") or False
        children = kwargs.get("children")
        kwargs.pop("operator") if "operator" in kwargs else None
        kwargs.pop("match_type") if "match_type" in kwargs else None
        kwargs.pop("node_type") if "node_type" in kwargs else None
        kwargs.pop("negated") if "negated" in kwargs else None
        kwargs.pop("children") if "children" in kwargs else None

        if children is None:
            children = list([arg for arg in args if isinstance(arg, self.__class__)]) + \
                       [C(node_type=MatchNode.BASE_NODE, operator=operator,
                          children=[{k: v} for k, v in kwargs.iteritems()],
                          match_type=match_type, negated=negated)]
        elif children is []:
            children = []

        super(C, self).__init__(node_type, operator, children, match_type, negated)
        # self.operator = C.default_operator

    def _combine(self, otherC, operator):
        if not isinstance(otherC, C):
            InterruptError("otherC is not a C")
        # self.add(otherC, operator)
        obj = self._clone()
        obj.add(otherC, operator)
        return obj
        # return self

    def __or__(self, other):
        return self._combine(other, MatchOperator.SHOULD)

    def __and__(self, other):
        return self._combine(other, MatchOperator.MUST)

    def __invert__(self):
        obj = type(self)()
        obj.add(self, MatchOperator.MUST_NOT)
        return obj

    def _clone(self, ):
        return copy.deepcopy(self)


class ConditionCompiler(object):
    default_condition_dict = {}
    execution_list = {}
    execution_result = {}
    execution_id = 0

    def __init__(self, model=None, compiler=None, condition_dict=None, query=None, sort_list=None):
        self.model = model
        if query:
            self.query = query
        else:
            self._init_query()
        self.condition_dict = condition_dict or {"query": query}
        self.compiler = compiler or Compiler()
        self.bool = None
        self.must_list = None
        self.must_not_list = None
        self.should_list = None
        self.sort = sort_list or []
        self._positions = []
        self.node = C()
        self.args = ()
        self.kwargs = {}

    def bool(self, boost=None, **kwargs):
        node = self.find_dsl_node()
        if not isinstance(node, dict):
            return "node is not a 'dict'"
        if "bool" not in node:
            node["bool"] = {}
        self.add_postion("bool")
        return self._clone()

    def __getitem__(self, item):
        if isinstance(item, int):
            node = self.find_dsl_node()[item]
            self.add_postion(item)
            return self._clone()
        else:
            return super(ConditionCompiler, self).__getitem__(item)

    def add_postion(self, data):
        self._positions.append(data)

    def find_dsl_node(self):
        condition = self.condition_dict
        for _index in self._positions:
            if isinstance(_index, int) or isinstance(_index, str):
                condition = condition[_index]
            else:
                pass
        return condition

    def filter(self, *args, **kwargs):

        return self._filter_or_exclude(False, *args, **kwargs)

    def _filter_or_exclude(self, negated=False, *args, **kwargs):
        for arg in args:
            if isinstance(arg, C):
                self.add_c(arg)
        if negated:
            self.add_c(C(node_type=MatchNode.BASE_NODE, operator=MatchOperator.MUST_NOT,
                         children=[{k: v} for k, v in kwargs.iteritems()],
                         match_type=MatchType.MATCH_PHRASE, negated=negated),
                       )
        else:
            self.add_c(C(a=1)
                       )
        return self._clone()

    def exclude(self, *args, **kwargs):
        # self._init_query_bool()
        # must_condition = [{match_type: {field: value}} for field, value in kwargs.iteritems()]
        # self.must_not_list.extend(must_condition)
        # return self._clone()
        return self._filter_or_exclude(True, *args, **kwargs)

    def _init_query(self):
        self.query = {"match_all": {}}

    def _init_query_bool(self):
        if "bool" not in self.query:
            self.must_list = []
            self.must_not_list = []
            self.should_list = []
            self.bool = {
                "must": self.must_list,
                "must_not": self.must_not_list,
                "should": self.should_list
            }
            self.query["bool"] = self.bool
            self.remove_match_all()

    def remove_match_all(self):
        self.query.pop("match_all")

    def result(self):
        return self.condition_dict

    def total(self):
        pass

    def _clone(self, ):
        return copy.deepcopy(self)

    def clone(self):
        return self._clone()

    def add_c(self, c_object):
        # if c_object.operator == MatchOperator.MUST:
        #     # self._init_query_bool()
        #     # must_condition = [
        #     #     {c_object.match_type: {field: value}}
        #     #     for field, value in c_object.kwargs.iteritems()
        #     #     ]
        #     # self.must_list.extend(must_condition)
        #     # return self._clone()
        #     self.node.add(c_object, operator=c_object.operator)
        # elif c_object.operator == MatchOperator.MUST_NOT:
        #     # self._init_query_bool()
        #     # must_condition = [
        #     #     {c_object.match_type: {field: value}}
        #     #     for field, value in c_object.kwargs.iteritems()
        #     #     ]
        #     # self.must_not_list.extend(must_condition)
        #     # return self._clone()
        #     self.node.add(c_object, operator=c_object.operator)
        # else:
        self.node.add(c_object, operator=c_object.operator)


class QuerySet(object):
    """
    docs operation
    """
    result = None

    def __init__(self, model, condition, index_name, doc_type_list):
        self.model = model
        self.condition = condition or ConditionCompiler(self.model).result()
        self.index_name = index_name
        self.doc_type_list = doc_type_list

    def __iter__(self):
        return iter(self.result)

    def total(self):
        return self.condition.total()

    def must(self):
        return self.condition.must()

    def clone(self):
        return self._clone()

    def filter(self):
        return self._clone()

    def must_not(self):
        return self._clone()

    def should(self):
        return self._clone()

    def _fetch_all(self):
        if self.result is None:
            self.result = list(self.iterator())

    def iterator(self):
        return []

    def _set_up(self):
        pass

    def _clone(self, _class=None, ):
        if _class is None:
            _class = self.__class__
        c = _class(model=self.model, condition=self.condition, index_name=self.index_name,
                   doc_type_list=self.doc_type_list)
        return c


if __name__ == "__main__":
    # c1 = C(operator=MatchOperator.MUST, children=[{"d": 0}, {"e": 100}],
    #        match_type=MatchType.MATCH)
    # c2 = C(operator=MatchOperator.MUST, children=[{"f": 11}, {"g": 111}])
    # c3 = C(operator=MatchOperator.MUST, children=[{"h": 23123}, {"i": 5465}],
    #        match_type=MatchType.MATCH)
    # cc1 = C(node_type=C.NODE, operator=MatchOperator.SHOULD, children=[c1, c2])
    # cc2 = C(node_type=C.NODE, operator=MatchOperator.SHOULD, children=[c3, c2])
    # c1 = C(d=0, e=0)
    # c2 = C(d=1, e=1)
    # c3 = C(d=2, e=2)
    # cc1 = c1 | c2
    # cc2 = c2 | c3
    # c = cc1 | cc2
    # print c.transform()
    con = ConditionCompiler().filter(C(c=1) | C(d=22), a=1)
    print json.dumps(con.node.transform())

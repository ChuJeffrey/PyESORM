#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018-8-27 下午5:01
# @Author  : Jeffrey
# @Site    : 
# @File    : models.py
# @Software: PyCharm
"""
功能：
"""

import datetime

import es_exceptions
from es_exceptions import InterruptError
from bases2 import DocQuerySet, C, ClsMgrMap

class FieldForm(object):
    pass


# -------------------------------- Field start ---------------------------------

class Field(object):
    def __init__(self, name=None, default=None, message=None, *args, **kwargs):
        self.name = name
        self.default = default
        self.error_messages = message
        self.value = self.default

    def to_python(self, value):
        return value

    def check(self):
        raise NotImplementedError


class IntegerField(Field):
    description = "Integer"

    def __init__(self, max_length=None, min_value=None, default=None, *args, **kwargs):
        super(IntegerField, self).__init__(max_length=max_length, min_value=min_value,
                                           default=default, *args, **kwargs)
        self.check()

    def _check_max_length_warning(self):
        if not (0 <= len(str(self.value)) < 20):
            es_exceptions.InterruptError("integer has invalid  max length")

    def to_python(self, value):
        if value is None:
            return value
        try:
            return int(value)
        except (TypeError, ValueError):
            raise es_exceptions.ValidationError(
                self.error_messages['invalid'],
                code='invalid',
                params={'value': value},
            )

    def get_internal_type(self):
        return "IntegerField"

    def get_prep_value(self):
        return int(self.default)

    def check(self):
        if not isinstance(self.default, int):
            return InterruptError("default  value is not integer type data")
        self._check_max_length_warning()


class KeywordField(Field):
    description = "Keyword"

    def __init__(self, max_length=None, min_value=None, default=None, *args, **kwargs):
        super(KeywordField, self).__init__(max_length=max_length, *args, **kwargs)
        self.check()

    def to_python(self, value):
        if value is None:
            return value
        try:
            return str(value)
        except (TypeError, ValueError):
            raise es_exceptions.ValidationError(
                self.error_messages['invalid'],
                code='invalid',
                params={'value': value},
            )

    def get_internal_type(self):
        return "KeywordField"

    def get_prep_value(self):
        return str(self.default)

    def check(self):
        if not isinstance(self.default, str):
            return InterruptError("default  value is not string type data")


class TextField(Field):
    description = "Text"

    def __init__(self, max_length=None, min_value=None, default=None, *args, **kwargs):
        super(TextField, self).__init__(max_length=max_length, default=default, *args, **kwargs)
        self.check()

    def to_python(self, value):
        if value is None:
            return value
        try:
            return str(value)
        except (TypeError, ValueError):
            raise es_exceptions.ValidationError(
                self.error_messages['invalid'],
                code='invalid',
                params={'value': value},
            )

    def get_internal_type(self):
        return "TextField"

    def get_prep_value(self):
        return str(self.default)

    def check(self):
        if not isinstance(self.default, str):
            return InterruptError("default  value is not string type data")


class DateTimeField(Field):
    description = "Text"
    DATETIME_TYPE = "datetime"
    DATETIME_STR_TYPE = "datetime_str"

    def __init__(self, max_length=None, min_value=None, default=None, *args, **kwargs):
        """
        :param max_length:
        :param min_value:
        :param default: datetime.datetime :
        :param args:
        :param kwargs:
        """
        super(DateTimeField, self).__init__(max_length=max_length, min_value=min_value,
                                            default=default, *args, **kwargs)
        self.default_format_type = DateTimeField.DATETIME_TYPE
        self.check()

    def to_python(self, value):
        if value is None:
            return value
        try:
            self.value = self.value.replace("T", " ")
            return datetime.datetime.strptime(self.value, "%Y-%m-%d")
        except (TypeError, ValueError):
            raise es_exceptions.ValidationError(
                self.error_messages['invalid'],
                code='invalid',
                params={'value': value},
            )

    def get_internal_type(self):
        return "TextField"

    def get_prep_value(self):
        return str(self.default)

    def _check_datetime_type(self):
        if isinstance(self.default, datetime.datetime):
            return True

    def _check_datetime_str_type(self):
        if isinstance(self.default, str):
            self.default_format_type = DateTimeField.DATETIME_STR_TYPE
            try:
                self.value = datetime.datetime.strptime(self.default, "%Y-%m-%d %H:%M:%S")
                return True
            except:
                return InterruptError("datetime_str can not cast to datetime")

    def check(self):
        if not self.default:
            return InterruptError("datetime default value should not be none")
        self._check_datetime_str_type()
        self._check_datetime_type()


# -------------------------------- Field end -----------------------------------


# -------------------------------- Manager start ----------------------------------

class BaseManager(DocQuerySet):
    pass


class Manager(BaseManager):
    pass




# -------------------------------- Manager end -----------------------------------



# -------------------------------- Model start ------------------------------------

class ModelBase(type):
    def __new__(cls, name, bases, attrs):
        super_new = super(ModelBase, cls).__new__
        # print cls.__name__, name, bases, attrs
        attrs = attrs if attrs else {}
        if "Meta" in attrs:
            doc_type = attrs["Meta"].doc_type
            attrs["objects"] = Manager(doc_type=doc_type)
            subclass = super_new(cls, name, bases, attrs)
            attrs["objects"].update_model_class(subclass)
            ClsMgrMap.add_map(doc_type, subclass, Manager)
        else:
            subclass = super_new(cls, name, bases, attrs)
        return subclass


def with_metaclass(meta, *bases):  #
    """Create a base class with a metaclass.
    通过构造类获取构造类的实例以及对构造类的实例进行一些额外操作(目测是闭包的思想)
    """

    class metaclass(meta):
        def __new__(cls, name, this_bases, d):
            return meta(name, bases, d)
    return type.__new__(metaclass, 'temporary_class', (), {})


class Model(with_metaclass(ModelBase)):
    pass


# -------------------------------- Model end -------------------------------------

class TestModel(Model):
    class Meta:
        doc_type = "video_search"
    video_title = TextField(default="1")
    hot = IntegerField(default=0)


if __name__ == "__main__":
    m = TestModel.objects.filter(hot__lt=131)[1:5]
    for r in m:
        print r

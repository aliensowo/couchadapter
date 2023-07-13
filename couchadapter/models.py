from __future__ import unicode_literals

import logging
import sys
from datetime import timedelta
from decimal import Decimal
from distutils.util import strtobool
from typing import Dict, List, Union

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, ClusterOptions, ClusterTimeoutOptions
from django.conf import settings
from django.db import models
from django.db.models.base import ModelBase
from django.db.models.fields import DateTimeField, DecimalField
from django.db.models.fields.files import FileField
from django.forms.models import model_to_dict
from django.http import HttpResponseNotFound
from django.utils import dateparse, timezone
from django_extensions.db.fields import ShortUUIDField
from six import string_types
from tastypie.serializers import Serializer

import couchadapter.mapper
from couchadapter.fields import ModelReferenceField
from couchadapter.management.djangotoolbox.fields import EmbeddedModelField, ListField
from couchadapter.preview_mode import PreviewMethods
from couchadapter.query import QueryResult
from couchadapter.specification import GroupbySpetification, Specification

logger = logging.getLogger(__name__)
CHANNELS_FIELD_NAME = "channels"
DOC_TYPE_FIELD_NAME = "doc_type"

CHANNEL_PUBLIC = "public"

attrs = (
    "scope",
    "collection",
)


# Create your models here.
class CouchbaseModelError(Exception):
    pass


class NotFoundError(Exception):
    pass


class CBModel(models.Model):
    class Meta:
        abstract = True

    class AboutModel:
        fields_name = tuple()

    try:
        _query_timeout = settings.QUERY_TO
    except NameError:
        _query_timeout = 120
    try:
        _kv_timeout = settings.KV_TO
    except NameError:
        _kv_timeout = 120
    try:
        _views_timeout = settings.VIEWS_TO
    except NameError:
        _views_timeout = 120
    try:
        _config_total_timeout = settings.CONFIG_TOT_TO
    except NameError:
        _config_total_timeout = 120

    childlist = set()
    id_prefix = "st"
    doc_type = None
    scope = None
    collection = None
    bucket = None
    _serializer = Serializer()
    db = None

    def __new__(cls, *args, **kwargs):
        instance = super(CBModel, cls).__new__(cls)
        if cls.db is None:
            cls.db = instance.get_cluster()
        return instance

    def __add_to_child_list__(self, child):
        self.childlist.add(child)
        return self.childlist

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.get_id() == other.get_id()

    def __str__(self):
        if hasattr(self, "pk"):
            return "%s object (%s)" % (self.__class__.__name__, self.pk)
        return "%s object" % (self.__class__.__name__,)

    def __init__(self, *args, **kwargs):
        self.created = None
        self.channels = []
        self.id = None
        self.db = None

        self.rev = None
        if self.db is None:
            self.db = self.get_cluster()
        if hasattr(self, "bucket"):
            self.collection = self.db.bucket(self.bucket).scope(self.scope).collection(self.collection)
        if "id_prefix" in kwargs:
            self.id_prefix = kwargs["id_prefix"]
            del kwargs["id_prefix"]

        if "id" in kwargs:
            self.id = kwargs["id"]
            del kwargs["id"]

        clean_kwargs = self.__clean_kwargs(kwargs)
        super(CBModel, self).__init__(**clean_kwargs)

        if len(args) == 1:
            v = args[0]
            if isinstance(v, string_types):
                self.load(v)

    def get_id(self):
        if self.is_new():
            pf = ShortUUIDField()
            self.id = self.id_prefix + "::" + pf.create_uuid()
        return self.id

    def get_cluster(self):
        opt = ClusterTimeoutOptions(
            query_timeout=timedelta(seconds=self._query_timeout),
            kv_timeout=timedelta(self._kv_timeout),
            views_timeout=timedelta(seconds=self._views_timeout),
            config_total_timeout=timedelta(self._config_total_timeout),
        )

        cluster = Cluster(
            "couchbase://%s" % settings.COUCHBASE_HOSTS,
            ClusterOptions(
                PasswordAuthenticator(settings.COUCHBASE_USER, settings.COUCHBASE_PASSWORD),
                timeout_options=opt,
            ),
        )
        return cluster

    def _save(self, *args, **kwargs):
        self.updated = timezone.now()
        if not hasattr(self, "created") or self.created is None:
            self.created = self.updated

        # save files
        for field in self._meta.fields:
            if isinstance(field, FileField):
                file_field = getattr(self, field.name)

                if not file_field._committed:
                    file_field.save(file_field.name, file_field, False)
        return self.to_dict()

    def save(self, *args, **kwargs):
        data_dict = self._save(*args, **kwargs)

        if isinstance(settings.DEBUG, str):
            debug = strtobool(settings.DEBUG)
        elif isinstance(settings.DEBUG, bool):
            debug = settings.DEBUG
        else:
            raise TypeError("Debug must be boolean or string")
        if debug:
            from couchadapter.indexer import QueryServiceManager

            preview_methods = PreviewMethods(self.db.bucket(self.bucket))
            preview_methods.make_scope(scope=self.scope)
            preview_methods.make_coll(scope=self.scope, collection_in_scope=self.collection)
            QueryServiceManager(self.__class__).a_inx_mng.create_primary_index(self.bucket)
        # data_dict.pop("id")
        if self.is_new():
            self.collection.append(self.id, data_dict)
        else:
            self.collection.upsert(self.id, data_dict)

    @classmethod
    def _save_multi(cls, entities: List["CBModel"]):

        current_class = entities[0].__class__
        for_save = {}
        for entity in entities:
            if entity.__class__ != current_class:
                raise ValueError("entities list should contain only one unique class")

            data_dict = entity._save()
            for_save[entity.get_id()] = data_dict

        entities[0].collection.upsert_multi(for_save)

    @classmethod
    def save_multi(cls, elements: list, i=1):
        """
        Вызвать метод и передать в него именованный аргумент elements

        :param elements: list of objects
        :param i:
        :return: -> count of parts list
        """
        MB_20 = 20000000
        if len(elements) == 0:
            raise IndexError("Elements len() < 1")
        elif len(elements) == 1:
            cls._save_multi([cls(**elements[-1])])
            return 1

        elem = []
        for item in elements:
            if not isinstance(item, cls):
                raise ValueError
            elem.append(item)

        if sys.getsizeof(elem) > MB_20:
            logger.info("initial verification::too large list object")

        i = i * 2
        if len(elements) % 2 != 0:
            try:
                cls._save_multi([cls(**elements[-1])])
            except Exception as e:
                logger.error(e)
                logger.error("methods retry")
            elements = elements[:-1]
        try:
            for j in range(1, i + 1):
                if j - 1 == 0:
                    elem = [elem for elem in elements[: int(len(elements) / i)]]
                else:
                    elem = [elem for elem in elements[int(len(elements) / i * (j - 1)) : int(len(elements) / i * j)]]
                if sys.getsizeof(elem) > MB_20:
                    logger.info("too large list object. divide in half")
                    i = cls.save_multi(i=i, elements=elements)
                else:
                    cls._save_multi(elem)
            return i
        except Exception as e:
            logger.error(e)
            i = cls.save_multi(i=i, elements=elements)

        return i

    # for saving
    def to_dict(self):
        d = model_to_dict(self)
        tastyjson = self._serializer.to_json(d)
        d = self._serializer.from_json(tastyjson)

        d[DOC_TYPE_FIELD_NAME] = self.get_doc_type()
        d["id"] = self.get_id()
        if "cbnosync_ptr" in d:
            del d["cbnosync_ptr"]
        if "csrfmiddlewaretoken" in d:
            del d["csrfmiddlewaretoken"]
        for field in self._meta.fields:
            if isinstance(field, DateTimeField):
                d[field.name] = self._string_from_date(field.name)
            if isinstance(field, ListField):
                if isinstance(field.item_field, EmbeddedModelField):
                    self.to_dict_nested_list(field.name, d)
                if isinstance(field.item_field, ModelReferenceField):
                    self.to_dict_reference_list(field.name, d)
            if isinstance(field, EmbeddedModelField):
                self.to_dict_nested(field.name, d)
            if isinstance(field, ModelReferenceField):
                self.to_dict_reference(field.name, d)
        d.pop("id")
        return d

    def from_dict(self, dict_payload):
        for field in self._meta.fields:
            if field.name not in dict_payload:
                continue
            if isinstance(field, EmbeddedModelField):
                self.from_dict_nested(field.name, field.embedded_model, dict_payload)
                continue
            if isinstance(field, ListField):
                if isinstance(field.item_field, EmbeddedModelField):
                    self.from_dict_nested_list(field.name, field.item_field.embedded_model, dict_payload)
            if isinstance(field, DateTimeField):
                self._date_from_string(field.name, dict_payload.get(field.name))
            elif isinstance(field, DecimalField):
                self._decimal_from_string(field.name, dict_payload.get(field.name))
            elif field.name in dict_payload:
                setattr(self, field.name, dict_payload[field.name])
        if "id" in dict_payload.keys():
            self.id = dict_payload["id"]

    def from_row(self, row):
        self.from_dict(row)

    def load(self, id):
        try:
            doc = self.collection.get(id)
            self.from_row(doc.content)
            self.id = id
        except Exception as e:
            logger.info(e)
            raise NotFoundError

    def load_list(self, doc):
        self.from_row(doc)

    def delete(self, **kwargs):
        try:
            for field in self._meta.fields:
                if isinstance(field, ModelReferenceField):
                    fld = getattr(self, field.name)
                    if isinstance(field, field.embedded_model):
                        fld.delete()
            self.collection.remove(self.id)
        except NotFoundError:
            return HttpResponseNotFound

    def load_related(self, related_attr, related_klass):
        id = getattr(self, related_attr)
        return related_klass(id)

    def load_related_list(self, related_attr, related_klass):
        ids = getattr(self, related_attr)
        docs_arr = related_klass.db.get_multi(ids)
        objs = []
        for doc in docs_arr:
            value = docs_arr[doc]
            objs.append(related_klass(value))
        return objs

    def to_dict_nested(self, key, parent_dict):
        parent_dict[key] = getattr(self, key).to_dict()
        return parent_dict

    def to_dict_nested_list(self, key, parent_dict):
        parent_dict[key] = []
        for item in getattr(self, key):
            parent_dict[key].append(item.to_dict())
        return parent_dict

    def to_dict_reference(self, key, parent_dict):
        ref_obj = getattr(self, key)
        if ref_obj and not isinstance(ref_obj, string_types):
            ref_obj.save()
            parent_dict[key] = ref_obj.id
        return parent_dict

    def to_dict_reference_list(self, key, parent_dict):
        ref_objs = getattr(self, key)
        id_arr = []
        if isinstance(ref_objs, list) and len(ref_objs):
            for obj in ref_objs:
                if obj and not isinstance(obj, string_types):
                    obj.save()
                    id_arr.append(obj.id)
        parent_dict[key] = id_arr
        return parent_dict

    def to_dict_partial_reference(self, key, parent_dict, links):
        ref_obj = getattr(self, key)
        if ref_obj and not isinstance(ref_obj, string_types):
            ref_obj.save()
            parent_dict[key] = ref_obj.id
            for key, value in links.iteritems():
                parent_dict[key] = getattr(ref_obj, value)
                pass
        return parent_dict

    def from_dict_nested(self, key, nested_klass, dict_payload):
        if key in dict_payload.keys():
            item = nested_klass()
            item.from_dict(dict_payload[key])
            nested_list = item
            setattr(self, key, nested_list)

    def from_dict_nested_list(self, key, nested_klass, dict_payload):
        setattr(self, key, [])
        nested_list = getattr(self, key)
        if key in dict_payload.keys():
            for d in dict_payload[key]:
                item = nested_klass()
                item.from_dict(d)
                nested_list.append(item)

    def append_to_references_list(self, key, value):
        v = getattr(self, key, [])

        if not isinstance(v, list):
            v = []

        if value not in v:
            v.append(value)

        setattr(self, key, v)

    def get_references_list(self, key):
        v = getattr(self, key, [])

        if not isinstance(v, list):
            v = []

        return v

    def delete_from_references_list(self, key, value):
        v = getattr(self, key, [])

        if not isinstance(v, list):
            v = []

        if value in v:
            v.remove(value)

        setattr(self, key, v)

    def is_new(self):
        return not hasattr(self, "id") or not self.id

    def from_json(self, json_payload):
        d = self._serializer.from_json(json_payload)
        self.from_dict(d)

    def _date_from_string(self, field_name, val):
        try:
            setattr(self, field_name, dateparse.parse_datetime(val))
        except Exception as e:
            setattr(self, field_name, val)
            logger.warning("can not parse date (raw value used) %s: %s", field_name, e)

    def _string_from_date(self, field_name):
        try:
            return getattr(self, field_name).isoformat()
        except Exception as e:
            logger.warning(e)
            return None

    def _decimal_from_string(self, field_name, val):
        try:
            setattr(self, field_name, Decimal(val))
        except Exception as e:
            setattr(self, field_name, val)
            logger.warning("can not parse decimal (raw value used) %s: %s", field_name, e)

    def to_json(self):
        d = self.to_dict()
        return self._serializer.to_json(d)

    def get_doc_type(self):
        if self.doc_type:
            return self.doc_type
        return self.__class__.__name__.lower()

    def __unicode__(self):
        return "%s: %s" % (self.id, self.to_json())

    def __clean_kwargs(self, data):
        common = set.intersection(
            {f.name for f in self._meta.get_fields()},
            data.keys(),
        )
        return {fname: data[fname] for fname in common}

    @classmethod
    def _filter(
        cls,
        expression: Specification,
        order_by: Union[Dict[str, str], None] = None,
        limit: Union[None, int] = None,
        offset: Union[None, int] = None,
    ) -> str:
        query = (
            f"select meta().id, * from `{cls.bucket}`.`{cls.scope}`.`{cls.collection}` where "
            + expression.is_satisfied_by("")
        )
        if order_by is not None and isinstance(order_by, dict):
            query += f" order by"
            for key, value in order_by.items():
                query += f" {key} {value},"
            query = query[:-1]  # delete unnecessary comma
        if limit is not None and isinstance(limit, int) and limit > 0:
            query += f" limit {limit}"
            if offset is not None and isinstance(limit, int) and offset >= 0:
                query += f" offset {offset}"
        return query

    @classmethod
    def filter(
        cls,
        expression: Specification,
        order_by: Union[Dict[str, str], None] = None,
        limit: Union[None, int] = None,
        offset: Union[None, int] = None,
        as_dict: bool = False,
    ):
        query = cls._filter(expression, order_by, limit, offset)

        logger.info(f"result filetr query: {query}")
        if as_dict:
            result = list(cls.db.query(query))
        else:
            obj = couchadapter.mapper.Mapper(cls)
            result = QueryResult(obj.mapper(query_result=list(cls.db.query(query))))
        return result

    @classmethod
    def _group_by(
        cls,
        field: str,
        agg_expression: GroupbySpetification,
        filter_expression: Specification = None,
        order_by: Union[Dict[str, str], None] = None,
        having_expression: Specification = None,
        limit: Union[None, int] = None,
        offset: Union[None, int] = None,
    ):
        query = (
            f"select {field},"
            + agg_expression.is_satisfied_by("").replace("and", ",")
            + f" from `{cls.bucket}`.`{cls.scope}`.`{cls.collection}`"
        )
        if filter_expression is not None:
            query += " where " + filter_expression.is_satisfied_by("")
        query += f" group by {field}"
        if having_expression is not None:
            query += f" having {having_expression.is_satisfied_by('')}"
        if order_by is not None and isinstance(order_by, dict):  # TODO вынести в другое место
            query += f" order by"
            for key, value in order_by.items():
                query += f" {key} {value},"
            query = query[:-1]  # delete unnecessary comma
        if limit is not None and isinstance(limit, int) and limit > 0:
            query += f" limit {limit}"
            if offset is not None and isinstance(limit, int) and offset >= 0:
                query += f" offset {offset}"
        return query

    @classmethod
    def group_by(
        cls,
        field: str,
        agg_expression: GroupbySpetification,
        filter_expression: Specification = None,
        order_by: Union[Dict[str, str], None] = None,
        having_expression: Specification = None,
        limit: Union[None, int] = None,
        offset: Union[None, int] = None,
    ):
        query = cls._group_by(
            field,
            agg_expression,
            filter_expression,
            order_by,
            having_expression,
            limit,
            offset,
        )
        logger.info(f"result group by query: {query}")
        return list(cls.db.query(query))

    @classmethod
    def _analytics(
        cls,
        expression: Specification,
        order_by: Union[Dict[str, str], None] = None,
        limit: Union[None, int] = None,
        offset: Union[None, int] = None,
    ):
        query = (
            f"select meta().id, * from `{cls.bucket}`.`{cls.scope}`.`{cls.collection}` where "
            + expression.is_satisfied_by("")
        )
        if order_by is not None and isinstance(order_by, dict):
            query += f" order by"
            for key, value in order_by.items():
                query += f" {key} {value},"
            query = query[:-1]  # delete unnecessary comma
        if limit is not None and isinstance(limit, int) and limit > 0:
            query += f" limit {limit}"
            if offset is not None and isinstance(limit, int) and offset >= 0:
                query += f" offset {offset}"
        return query

    @classmethod
    def analytics(
        cls,
        expression: Specification,
        order_by: Union[Dict[str, str], None] = None,
        limit: Union[None, int] = None,
        offset: Union[None, int] = None,
        as_dict: bool = False,
    ):

        query = cls._analytics(expression, order_by, limit, offset)
        logger.info(f"result analytics query: {query}")
        if as_dict:
            result = list(cls.db.analytics_query(query))
        else:
            obj = couchadapter.mapper.Mapper(cls)
            result = QueryResult(obj.mapper(query_result=list(cls.db.analytics_query(query))))
        return result


class CBNestedModel(CBModel):
    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        super(CBNestedModel, self).__init__(*args, **kwargs)

    def save(self, *args, **kwargs):
        raise CouchbaseModelError("this object is not supposed to be saved, it is nested")

    def load(self, id):
        raise CouchbaseModelError("this object is not supposed to be loaded, it is nested")

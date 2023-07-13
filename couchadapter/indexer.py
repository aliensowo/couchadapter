import importlib
from typing import AnyStr, Union

from couchbase.analytics import AnalyticsDataType
from django.db.models.base import ModelBase

from couchadapter.models import CBModel

DATA_TYPES = {
    "BigIntegerField": AnalyticsDataType.INT64,
    "IntegerField": AnalyticsDataType.INT64,
    "FloatField": AnalyticsDataType.DOUBLE,
    "TextField": AnalyticsDataType.STRING,
    "CharField": AnalyticsDataType.STRING,
    "EmailField": AnalyticsDataType.STRING,
}


class IndexServiceManager:
    def __init__(self, model: Union[AnyStr, CBModel, ModelBase] = None):
        if isinstance(model, str):
            if "." in model:
                module, func = model.rsplit(".", 1)
                m = importlib.import_module(module)
                self.model: Union[ModelBase, CBModel] = getattr(m, func)
            else:
                raise Exception("Expected CBModel\nBut path cant find a '.'(dot) symbol")
        elif isinstance(model.__new__(model), CBModel) and isinstance(model, ModelBase):
            self.model: Union[ModelBase, CBModel] = model
        else:
            raise Exception("Expected CBModel but value not found")
        self.cluster = self.model.__new__(self.model).get_cluster()

    def create_secondary(self, *args, **kwargs):
        pass

    def drop_secondary(self, *args, **kwargs):
        pass

    def create_primary(self, *args, **kwargs):
        pass

    def drop_primary(self, *args, **kwargs):
        pass


class AnalyticsServiceManager(IndexServiceManager):
    def __init__(self, model):
        super(AnalyticsServiceManager, self).__init__(model)
        self.a_inx_mng = self.cluster.analytics_indexes()

    def create_dataverse(self):
        a_inx_mng = self.cluster.analytics_indexes()
        dataverse_name = f"{self.model.bucket}/{self.model.scope}"
        a_inx_mng.create_dataverse(dataverse_name=dataverse_name, ignore_if_exists=True)
        return dataverse_name

    def create_dataset(self, ignore_if_exists):
        dataverse_name = self.create_dataverse()
        self.a_inx_mng.create_dataset(
            dataset_name=self.model.collection,
            bucket_name=self.model.bucket,
            dataverse_name=dataverse_name,
            ignore_if_exists=ignore_if_exists,
        )

    def create_secondary(self, field, idx_name=None, ignore_if_exists=True):
        self.create_dataset(ignore_if_exists)
        if field in self.model.AboutModel.fields_name:
            attr = getattr(self.model, field)
            try:
                f_type = DATA_TYPES[attr.field.__class__.__name__]
            except KeyError:
                raise KeyError("This field type not implemented yet.")
            self.a_inx_mng.create_index(
                index_name=f"{self.model.bucket}_" f"{self.model.collection}_aindx_{field}",
                dataset_name=self.model.collection,
                fields={field: f_type},
                dataverse_name=f"{self.model.bucket}/{self.model.scope}",
                ignore_if_exists=ignore_if_exists,
            )

    def drop_secondary(self, idx_name):
        self.a_inx_mng.drop_index(
            idx_name, dataset_name=self.model.collection, dataverse_name=f"{self.model.bucket}/{self.model.scope}"
        )


class QueryServiceManager(IndexServiceManager):
    def __init__(self, model):
        super(QueryServiceManager, self).__init__(model)
        self.a_inx_mng = self.cluster.query_indexes()

    def create_secondary(self, *fields, idx_name=None):
        if idx_name is None:
            idx_name = f"{self.model.bucket}_{self.model.collection}_adv_"
            idx_name += "_".join(["{}".format(field) for field in fields])
        self.a_inx_mng.create_index(self.model.bucket, idx_name, fields=fields)

    def create_primary(self):
        self.a_inx_mng.create_primary_index(self.model.bucket)

    def drop_primary(self):
        self.a_inx_mng.drop_primary_index(self.model.bucket)

    def drop_secondary(self, idx_name):
        self.a_inx_mng.drop_index(self.model.bucket, idx_name)

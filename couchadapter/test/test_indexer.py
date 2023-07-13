from unittest import TestCase

from couchadapter.indexer import AnalyticsServiceManager, QueryServiceManager
from couchadapter.test.author_fixture.models import *


class IndexerTest(TestCase):
    def setUp(self):
        set_up_author_example()

    def test_analytics_indexer_by_model(self):
        AnalyticsServiceManager(model="couchadapter.test.author_fixture.models.Book").create_secondary(field="name")
        AnalyticsServiceManager(model=Book).drop_secondary("unit_test_bucket__default_aindx_name")

    def test_analytics_indexer_by_text(self):
        AnalyticsServiceManager(model=Book).create_secondary(field="pages")
        AnalyticsServiceManager(model=Book).drop_secondary("unit_test_bucket__default_aindx_pages")

    def test_query_indexer_by_model(self):
        QueryServiceManager(model="couchadapter.test.author_fixture.models.Book").create_secondary("name")
        QueryServiceManager(model=Book).drop_secondary("unit_test_bucket__default_adv_name")

    def test_query_indexer_by_text(self):
        QueryServiceManager(model=Book).create_secondary("pages")
        QueryServiceManager(model=Book).drop_secondary("unit_test_bucket__default_adv_pages")

    def test_query_indexer_with_multiple_fields(self):
        QueryServiceManager(model=Book).create_secondary("pages", "name")
        QueryServiceManager(model=Book).drop_secondary("unit_test_bucket__default_adv_pages_name")

    def tearDown(self):
        dict_teardown()

from unittest import TestCase

from couchadapter.indexer import AnalyticsServiceManager
from couchadapter.test.author_fixture.models import *


class AnalyticsTest(TestCase):
    def setUp(self):
        set_up_author_example()
        AnalyticsServiceManager(model="couchadapter.test.author_fixture.models.Book").create_secondary(field="name")

    def test_query(self):
        query = EqualFilter(pages=250)
        book = Book.analytics(expression=query)
        self.assertEqual(book.first().pages, 250)

    def tearDown(self):
        dict_teardown()
        AnalyticsServiceManager(model=Book).drop_secondary("unit_test_bucket__default_aindx_name")

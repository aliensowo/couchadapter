from unittest import TestCase

from couchadapter import mapper
from couchadapter.test.author_fixture.models import *


class QueryResultTest(TestCase):
    def setUp(self):
        set_up_author_example()

    def test_first(self):
        query = EqualFilter(pages=250)
        book = Book.filter(expression=query)
        self.assertEqual(book.first().pages, 250)

    def test_all(self):
        query = EqualFilter(pages=250)
        book_qr = Book.filter(expression=query)
        book_raw = Book.filter(expression=query, as_dict=True)
        self.assertEqual(book_qr.all(), mapper.Mapper(Book).mapper(book_raw))

    def test_get(self):
        query = EqualFilter(pages=250)
        book = Book.filter(expression=query)
        self.assertIsNotNone(book.get({"name": "First Book"}))

    def tearDown(self):
        dict_teardown()

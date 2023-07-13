from unittest import TestCase

from couchadapter.specification import (
    Avg,
    Count,
    EqualNestedFilter,
    GreaterFilter,
    LessFilter,
    Max,
    Min,
    NotNullFilter,
    Sum,
)
from couchadapter.test.author_fixture.models import *


class FilterTest(TestCase):
    def setUp(self):
        set_up_author_example()

    def test_get_by_nested_key(self):

        query = EqualNestedFilter("nested_dict", tw=False)
        doc = Author.filter(query, as_dict=True)[0]
        self.assertEqual(doc["_default"]["nested_dict"]["tw"], False)

    def test_get_by_id(self):
        author = Author("test_id")
        self.assertEqual(author.name, "Aswin")

    def test_equal_filter_by_string(self):
        query = EqualFilter(name="Aswin")
        author = Author.filter(expression=query, as_dict=True)
        self.assertEqual(len(author), 1)
        self.assertEqual(author[0][Author.collection]["name"], "Aswin")

    def test_equal_filter_by_number(self):
        query = EqualFilter(pages=250)
        book = Book.filter(expression=query, as_dict=True)
        self.assertEqual(len(book), 1)
        self.assertEqual(book[0][Book.collection]["pages"], 250)

    def test_greater_filter(self):
        query = GreaterFilter(pages=300)
        book = Book.filter(expression=query, as_dict=True)
        self.assertEqual(len(book), 1)
        self.assertGreater(book[0][Book.collection]["pages"], 300)
        query = GreaterFilter(pages=200)
        books = Book.filter(expression=query, as_dict=True)
        self.assertEqual(len(books), 2)
        for book in books:
            self.assertGreater(book[Book.collection]["pages"], 200)

    def test_less_filter(self):
        query = LessFilter(pages=300)
        book = Book.filter(expression=query, as_dict=True)
        self.assertEqual(len(book), 1)
        self.assertLess(book[0][Book.collection]["pages"], 300)
        query = LessFilter(pages=500)
        books = Book.filter(expression=query, as_dict=True)
        self.assertEqual(len(books), 2)
        for book in books:
            self.assertLess(book[Book.collection]["pages"], 500)

    def test_and_filter(self):
        query = GreaterFilter(pages=300) & EqualFilter(name="Second Book")
        book = Book.filter(expression=query, as_dict=True)
        self.assertEqual(len(book), 1)
        self.assertGreater(book[0][Book.collection]["pages"], 300)
        self.assertEqual(book[0][Book.collection]["name"], "Second Book")

    def test_or_filter(self):
        query = GreaterFilter(pages=300) | EqualFilter(name="First Book")
        book = Book.filter(expression=query, as_dict=True)
        self.assertEqual(len(book), 2)

    def test_empty_result(self):
        query = GreaterFilter(pages=1000)
        book = Book.filter(expression=query, as_dict=True)
        self.assertEqual(len(book), 0)

    def test_limit_filter(self):
        query = GreaterFilter(pages=1)
        book = Book.filter(expression=query, limit=1, as_dict=True)
        self.assertEqual(len(book), 1)
        book = Book.filter(expression=query, limit=2, as_dict=True)
        self.assertEqual(len(book), 2)

    def test_limit_offset_filter(self):
        query = GreaterFilter(pages=1)
        book = Book.filter(expression=query, limit=2, offset=0, as_dict=True)
        self.assertEqual(len(book), 2)
        query = GreaterFilter(pages=1)
        book = Book.filter(expression=query, limit=2, offset=1, as_dict=True)
        self.assertEqual(len(book), 1)
        book = Book.filter(expression=query, limit=1, offset=2, as_dict=True)
        self.assertEqual(len(book), 0)

    def test_order_by_filter(self):
        query = GreaterFilter(pages=1)
        book = Book.filter(expression=query, order_by={"pages": "asc"}, as_dict=True)
        self.assertEqual(book[0][Book.collection]["name"], "First Book")
        self.assertEqual(book[1][Book.collection]["name"], "Second Book")

    def test_order_by_multi_filter(self):
        query = GreaterFilter(pages=1)
        book = Book.filter(expression=query, order_by={"pages": "asc", "name": "desc"}, as_dict=True)
        self.assertEqual(book[0][Book.collection]["name"], "First Book")
        self.assertEqual(book[1][Book.collection]["name"], "Second Book")

    def test_order_by_with_limit_filter(self):
        query = GreaterFilter(pages=1)
        book = Book.filter(expression=query, order_by={"pages": "asc"}, limit=2, as_dict=True)
        self.assertEqual(book[0][Book.collection]["name"], "First Book")
        self.assertEqual(book[1][Book.collection]["name"], "Second Book")
        book = Book.filter(expression=query, order_by={"pages": "asc"}, limit=1, offset=1, as_dict=True)
        self.assertEqual(book[0][Book.collection]["name"], "Second Book")

    def test_group_by_count(self):
        book = Book.group_by(
            field="doc_type",
            agg_expression=Count(field="*", alias="count_by_doc_type"),
            filter_expression=EqualFilter(doc_type="book"),
        )
        self.assertEqual(book[0]["count_by_doc_type"], 2)

    def test_group_by_max(self):
        book = Book.group_by(
            field="doc_type",
            agg_expression=Max(field="pages", alias="max_pages"),
            filter_expression=EqualFilter(doc_type="book"),
            order_by={"max_pages": "desc"},
            limit=1,
        )
        self.assertEqual(book[0]["max_pages"], 340)

    def test_group_by_max_and_count(self):
        book = Book.group_by(
            field="doc_type",
            agg_expression=Count(field="*", alias="count_by_doc_type") & Max(field="pages", alias="max_pages"),
            filter_expression=EqualFilter(doc_type="book"),
        )
        self.assertEqual(book[0]["count_by_doc_type"], 2)
        self.assertEqual(book[0]["max_pages"], 340)

    def test_group_by_min(self):
        book = Book.group_by(
            field="doc_type",
            agg_expression=Min(field="pages", alias="min_pages"),
            filter_expression=EqualFilter(doc_type="book"),
            order_by={"min_pages": "asc"},
            limit=1,
        )
        self.assertEqual(book[0]["min_pages"], 250)

    def test_group_by_sum(self):
        book = Book.group_by(
            field="doc_type",
            agg_expression=Sum(field="pages", alias="sum_pages"),
            filter_expression=EqualFilter(doc_type="book"),
            order_by={"sum_pages": "asc"},
        )
        self.assertEqual(book[0]["sum_pages"], 590)

    def test_group_by_avg(self):
        book = Book.group_by(
            field="doc_type",
            agg_expression=Avg(field="pages", alias="avg_pages"),
            filter_expression=EqualFilter(doc_type="book"),
            order_by={"avg_pages": "asc"},
            limit=1,
        )
        self.assertEqual(book[0]["avg_pages"], 295)

    def test_notnull_filter(self):
        query = NotNullFilter(pages="null")
        book = Book.filter(expression=query, as_dict=True)
        self.assertEqual(len(book), 0)
        query = NotNullFilter(pages="not null")
        book = Book.filter(expression=query, as_dict=True)
        self.assertEqual(len(book), 2)
        query = NotNullFilter(pages="some shit")
        self.assertRaises(ValueError, Book.filter, expression=query)

    def tearDown(self):
        dict_teardown()

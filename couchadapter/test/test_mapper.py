from unittest import TestCase

from couchadapter.mapper import get_class_name
from couchadapter.specification import GreaterFilter
from couchadapter.test.author_fixture.models import *


class MapperTest(TestCase):
    def setUp(self):
        set_up_author_example()

    def test_model_name(self):
        self.assertEqual(Author.__name__, get_class_name(Author))

    def test_empty_result(self):
        query = GreaterFilter(pages=1000)
        book = Book.filter(expression=query)
        self.assertEqual(book, [])

    def test_nested_ex(self):
        query = EqualFilter(url="4sw.in")
        book = Blog.filter(expression=query)
        self.assertEqual(book, [])

    def test_attr_objs_for_one(self):
        query = EqualFilter(pages=250)
        book = Book.filter(expression=query)
        self.assertEqual(book.first().name, "First Book")

    def test_update(self):
        query = EqualFilter(pages=250)
        book = Book.filter(expression=query).first()
        book.pages = 260
        book.save()
        query = EqualFilter(pages=250)
        book = Book.filter(expression=query)
        self.assertEqual(len(book), 0)
        query = EqualFilter(pages=260)
        book = Book.filter(expression=query).first()
        self.assertEqual(book.pages, 260)
        book.pages = 250
        book.save()

    def test_attr_obj_for_two_n_more(self):
        query = EqualFilter(doc_type="book")
        book = Book.filter(expression=query)
        self.assertEqual(len(book), 2)
        if book.first().name == "First Book":
            self.assertEqual(book[0].pages, 250)
        else:
            self.assertEqual(book[0].pages, 340)

        if book[0].name == "Second Book":
            self.assertEqual(book.first().pages, 340)
        else:
            self.assertEqual(book.first().pages, 250)

    def tearDown(self):
        mapper_teardown()

from unittest import TestCase

from couchadapter.test.author_fixture.models import *


class SaveMultiTest(TestCase):
    def test_save_multi(self):
        pub = Publisher(name="Famous Publications")
        book = Book(name="First Book", pages=251, publisher=pub)
        book2 = Book(name="Second Book", pages=252, publisher=pub)
        book3 = Book(name="Third Book", pages=253, publisher=pub)
        book4 = Book(name="fourth Book", pages=254, publisher=pub)
        Book.save_multi([book, book2, book3, book4])
        pub.delete()
        book.delete()
        book2.delete()
        book3.delete()
        book4.delete()

    def test_save_multi_different_models(self):
        pub = Publisher(name="Famous Publications")
        book = Book(name="First Book", pages=251, publisher=pub)
        book2 = Book(name="Second Book", pages=252, publisher=pub)
        book3 = Book(name="Third Book", pages=253, publisher=pub)
        book4 = Book(name="fourth Book", pages=254, publisher=pub)
        with self.assertRaises(ValueError):
            Book.save_multi([book, book2, book3, book4, pub])

    def test_save_multi_empty_list(self):
        with self.assertRaises(IndexError):
            Book.save_multi([])

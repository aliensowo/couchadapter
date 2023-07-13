from django.db import models

from couchadapter.fields import ModelReferenceField
from couchadapter.management.djangotoolbox.fields import DictField, EmbeddedModelField, ListField
from couchadapter.models import CBModel, CBNestedModel
from couchadapter.specification import EqualFilter


class Article(CBNestedModel):
    class Meta:
        abstract = True

    class AboutModel:
        fields_name = ("title",)

    bucket = "unit_test_bucket"
    scope = "_default"
    collection = "_default"
    doc_type = "blog"
    id_prefix = "blg"

    title = models.CharField(max_length=45, null=True, blank=True)


class Blog(CBNestedModel):
    class Meta:
        abstract = True

    class AboutModel:
        fields_name = (
            "articles",
            "url",
        )

    bucket = "unit_test_bucket"
    scope = "_default"
    collection = "_default"
    doc_type = "publisher"
    id_prefix = "pub"

    url = models.CharField(max_length=45, null=True, blank=True)
    articles = ListField(EmbeddedModelField(Article))


class Publisher(CBModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        super(Publisher, self).__add_to_child_list__(self.__class__)

    class Meta:
        abstract = True

    class AboutModel:
        fields_name = ("name",)

    bucket = "unit_test_bucket"
    scope = "_default"
    collection = "_default"
    doc_type = "publisher"
    id_prefix = "pub"

    name = models.CharField(max_length=45, null=True, blank=True)


class Book(CBModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        super(Book, self).__add_to_child_list__(self.__class__)

    class Meta:
        abstract = True

    class AboutModel:
        fields_name = "name", "pages", "publisher"

    bucket = "unit_test_bucket"
    scope = "_default"
    collection = "_default"
    doc_type = "book"
    id_prefix = "bk"

    name = models.CharField(max_length=45, null=True, blank=True)
    pages = models.IntegerField()
    publisher = ModelReferenceField(Publisher)


class Address(CBModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        super(Address, self).__add_to_child_list__(self.__class__)

    class Meta:
        abstract = True

    class AboutModel:
        fields_name = (
            "street",
            "city",
        )

    bucket = "unit_test_bucket"
    scope = "_default"
    collection = "_default"
    doc_type = "address"
    id_prefix = "addr"

    street = models.CharField(max_length=45, null=True, blank=True)
    city = models.CharField(max_length=45, null=True, blank=True)


class Author(CBModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        super(Author, self).__add_to_child_list__(self.__class__)

    class Meta:
        abstract = True

    class AboutModel:
        fields_name = "name", "blog", "books", "address", "nested_dict"

    bucket = "unit_test_bucket"
    scope = "_default"
    collection = "_default"
    doc_type = "author"
    id_prefix = "atr"

    name = models.CharField(max_length=45, null=True, blank=True)
    blog = EmbeddedModelField(Blog)
    books = ListField(ModelReferenceField(Book))
    address = ModelReferenceField(Address)
    nested_dict = DictField(name="nested_dict", verbose_name="nested_dict", max_length=100)


def set_up_author_example():
    article = Article(title="New Article")
    article2 = Article(title="Second Article")

    blog = Blog(url="4sw.in", articles=[article, article2])

    pub = Publisher(name="Famous Publications")
    pub2 = Publisher(name="Much more Famous Publications")

    book = Book(name="First Book", pages=250, publisher=pub)
    book2 = Book(name="Second Book", pages=340, publisher=pub2)

    address = Address(street="Anna Nagar", city="Chennai")

    nested_dict = {"tw": False, "vk": True, "yt": False}

    author = Author(
        name="Aswin",
        blog=blog,
        books=[book, book2],
        address=address,
        nested_dict=nested_dict,
    )
    author.id = "test_id"
    author.save()
    try:
        author.db.query("CREATE PRIMARY INDEX ON `default`:`unit_test_bucket`.`_default`.`_default`").execute()
    except:
        pass


def mapper_teardown():
    Publisher.filter(expression=EqualFilter(name="Famous Publications")).first().delete()
    Publisher.filter(expression=EqualFilter(name="Much more Famous Publications")).first().delete()
    Book.filter(expression=EqualFilter(name="First Book")).first().delete()
    Book.filter(expression=EqualFilter(name="Second Book")).first().delete()
    Address.filter(expression=EqualFilter(street="Anna Nagar")).first().delete()
    Author("test_id").delete()


def dict_teardown():
    pub = Publisher.filter(expression=EqualFilter(name="Famous Publications"), as_dict=True)
    Publisher(pub[0]["id"]).delete()
    pub = Publisher.filter(expression=EqualFilter(name="Much more Famous Publications"), as_dict=True)
    Publisher(pub[0]["id"]).delete()
    book = Book.filter(expression=EqualFilter(name="First Book"), as_dict=True)
    Book(book[0]["id"]).delete()
    book = Book.filter(expression=EqualFilter(name="Second Book"), as_dict=True)
    Book(book[0]["id"]).delete()
    addr = Address.filter(expression=EqualFilter(street="Anna Nagar"), as_dict=True)
    Address(addr[0]["id"]).delete()
    author = Author("test_id")
    author.delete()

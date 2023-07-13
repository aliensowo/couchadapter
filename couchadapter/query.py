class QueryResult(list):
    def __init__(self, objects):
        super().__init__()
        self.objects = objects

    def __len__(self):
        return len(self.objects)

    def __getitem__(self, i):
        return self.objects[i]

    def __iter__(self):
        for elem in self.objects:
            yield elem

    def __str__(self):
        if self.objects is not None:
            if len(self.objects) > 0:
                return "%s of <%s>" % (
                    self.__class__.__name__,
                    self.objects[0].__str__(),
                )
        return "<Empty %s>" % self.__class__.__name__

    def first(self):
        return self.objects[0]

    def get(self, expression):
        """The method may/will be changed in the future"""
        for obj in self.objects:
            for key, value in expression.items():
                if hasattr(obj, key):
                    if getattr(obj, key) == value:
                        return obj
        return None

    def all(self):
        return list(self)

    def update(self, expression):
        pass

    def filter(self, expression):
        pass

    def delete(self, expression):
        pass

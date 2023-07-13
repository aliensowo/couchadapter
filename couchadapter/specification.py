class Specification:
    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)

    def is_satisfied_by(self, candidate):
        raise NotImplementedError


class NestedSpecification(Specification):
    def is_satisfied_by(self, candidate):
        raise NotImplementedError


class CompositeSpecification(Specification):
    def is_satisfied_by(self, candidate):
        raise NotImplementedError


class MultaryCompositeSpecification(CompositeSpecification):
    def __init__(self, *specifications):
        self.specifications = specifications

    def is_satisfied_by(self, candidate):
        raise NotImplementedError


class And(MultaryCompositeSpecification):
    def __and__(self, other):
        if isinstance(other, And):
            self.specifications += other.specifications
        else:
            self.specifications += (other,)
        return self

    def is_satisfied_by(self, candidate):
        satisfied = " ".join(specification.is_satisfied_by(candidate) + "and" for specification in self.specifications)
        satisfied = satisfied[:-3]
        return satisfied

    def remainder_unsatisfied_by(self, candidate):
        non_satisfied = [
            specification for specification in self.specifications if not specification.is_satisfied_by(candidate)
        ]
        if not non_satisfied:
            return None
        if len(non_satisfied) == 1:
            return non_satisfied[0]
        if len(non_satisfied) == len(self.specifications):
            return self
        return And(*non_satisfied)


class Or(MultaryCompositeSpecification):
    def __or__(self, other):
        if isinstance(other, Or):
            self.specifications += other.specifications
        else:
            self.specifications += (other,)
        return self

    def is_satisfied_by(self, candidate):
        satisfied = " ".join(specification.is_satisfied_by(candidate) + "or" for specification in self.specifications)
        satisfied = satisfied[:-2]
        return satisfied


class UnaryCompositeSpecification(CompositeSpecification):
    def __init__(self, specification):
        self.specification = specification

    def is_satisfied_by(self, candidate):
        raise NotImplementedError


class BinaryCompositeSpecification(CompositeSpecification):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def is_satisfied_by(self, candidate):
        raise NotImplementedError


class NullaryCompositeSpecification(CompositeSpecification):
    def is_satisfied_by(self, candidate):
        raise NotImplementedError


class EqualFilter(Specification):
    def __init__(self, **fields):
        self.fields = fields

    def is_satisfied_by(self, candidate):
        query = ""
        for key, value in self.fields.items():
            if isinstance(value, str):
                query = f"""{key.replace("'", "''")}='{value.replace("'", "''")}' """
            else:
                query = f"""{key.replace("'", "''")}={value} """
        return query


class GreaterFilter(Specification):
    def __init__(self, **fields):
        self.fields = fields

    def is_satisfied_by(self, candidate):
        query = ""
        for key, value in self.fields.items():
            query += f"""{key.replace("'", "''")}>{value} """
        return query


class LessFilter(Specification):
    def __init__(self, **fields):
        self.fields = fields

    def is_satisfied_by(self, candidate):
        query = ""
        for key, value in self.fields.items():
            query = f"""{key.replace("'", "''")}<{value} """
        return query


class NotNullFilter(Specification):
    def __init__(self, **fields):
        self.fields = fields

    def is_satisfied_by(self, candidate):
        query = ""
        for key, value in self.fields.items():
            if str(value).lower() not in ["null", "not null"]:
                raise ValueError('Value must be "null" or "not null"')
            query += f"""{key.replace("'", "''")} is {value} """
        return query


class GroupbySpetification(Specification):
    def __init__(self, field, alias=None, distinct=False):
        self.distinct = distinct
        self.field = field
        self.alias = alias

    def is_satisfied_by(self, candidate):
        raise NotImplementedError


class Count(GroupbySpetification):
    def __init__(self, field, alias=None, distinct=False):
        super().__init__(field, alias, distinct)

    def is_satisfied_by(self, candidate):
        query = f""" count({self.field.replace("`", "``").replace("'", "''")}) """
        if self.distinct:
            query = query.replace("(", "(distinct ")
        if self.alias is not None:
            query += f"""as {self.alias.replace("'", "''")} """
        return query


class Avg(GroupbySpetification):
    def __init__(self, field, alias=None, distinct=False):
        super().__init__(field, alias, distinct)

    def is_satisfied_by(self, candidate):
        query = f""" avg({self.field.replace("`", "``").replace("'", "''")}) """
        if self.distinct:
            query = query.replace("(", "(distinct ")
        if self.alias is not None:
            query += f"""as {self.alias.replace("'", "''")} """
        return query


class Sum(GroupbySpetification):
    def __init__(self, field, alias=None, distinct=False):
        super().__init__(field, alias, distinct)

    def is_satisfied_by(self, candidate):
        query = f""" sum({self.field.replace("`", "``").replace("'", "''")}) """
        if self.distinct:
            query = query.replace("(", "(distinct ")
        if self.alias is not None:
            query += f"""as {self.alias.replace("'", "''")} """
        return query


class Min(GroupbySpetification):
    def __init__(self, field, alias=None, distinct=False):
        super().__init__(field, alias, distinct)

    def is_satisfied_by(self, candidate):
        query = f""" min({self.field.replace("`", "``").replace("'", "''")}) """
        if self.distinct:
            query = query.replace("(", "(distinct ")
        if self.alias is not None:
            query += f"""as {self.alias.replace("'", "''")} """
        return query


class Max(GroupbySpetification):
    def __init__(self, field, alias=None, distinct=False):
        super().__init__(field, alias, distinct)

    def is_satisfied_by(self, candidate):
        query = f""" max({self.field.replace("`", "``").replace("'", "''")}) """
        if self.distinct:
            query = query.replace("(", "(distinct ")
        if self.alias is not None:
            query += f"""as {self.alias.replace("'", "''")} """
        return query


class EqualNestedFilter(NestedSpecification):
    def __init__(self, dict_name: str, **fields):
        self.fields = fields
        self.dict_name = dict_name

    def is_satisfied_by(self, candidate):
        query = ""
        for key, value in self.fields.items():
            if isinstance(value, str):
                query = f"""{self.dict_name}.{key.replace("'", "''")} = '{value.replace("'", "''")}' """
            else:
                query = f"""{self.dict_name}.{key.replace("'", "''")} = {value} """
        return query


class GreaterNestedFilter(NestedSpecification):
    def __init__(self, dict_name: str, **fields):
        self.fields = fields
        self.dict_name = dict_name

    def is_satisfied_by(self, candidate):
        query = ""
        for key, value in self.fields.items():
            query += f"""{self.dict_name}.{key.replace("'", "''")}>{value} """
        return query


class LessNestedFilter(NestedSpecification):
    def __init__(self, dict_name: str, **fields):
        self.fields = fields
        self.dict_name = dict_name

    def is_satisfied_by(self, candidate):
        query = ""
        for key, value in self.fields.items():
            query = f"""{self.dict_name}.{key.replace("'", "''")}<{value} """
        return query

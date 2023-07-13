import inspect


class Mapper:
    object = None
    model_body = None

    def __init__(self, object):
        self.object = object
        self.model_body = {}
        self.get_field_type()

    def get_field_type(self):
        attr_fields_name = self.object.AboutModel.fields_name
        for elem in attr_fields_name:
            field_type_attr = getattr(self.object, elem).field
            self.model_body[field_type_attr.name] = {
                "type": field_type_attr.__class__.__name__,
                "value": None,
            }

    def mapper(self, query_result: list) -> list or None:
        if len(query_result) == 0:
            return []
        obj_list = []

        model_map = get_models_map([self.object])
        model_name = list(model_map.keys())[0]

        if model_map[model_name]["type"] == "CBNestedModel":
            return None

        for elem in query_result:
            elem_meta_id = elem["id"]
            ex = model_name(elem_meta_id)
            obj_list.append(ex)
        return obj_list


def get_class_name(obj: object):
    return obj.__name__


def get_models_map(models_list):
    total = {}
    for model in models_list:
        q = Mapper(object=model)
        total[model] = {
            "type": inspect.getmro(model)[1].__name__,
            "value": q.model_body,
        }
    return total

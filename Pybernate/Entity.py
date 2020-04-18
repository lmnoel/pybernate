from Pybernate.Exceptions import LazyInitializationException

class Entity:
    def __init__(self):
        self.persisted = False
        self.dirty = False

    def set_persisted(self):
        self.persisted = True

    def get_persisted(self):
        return self.persisted

    def set_dirty(self, state):
        self.dirty = state

    def get_dirty(self):
        return self.persisted

    def rollback(self):
        pass

class IdEntity(Entity):
    def __init__(self, **kwargs):
        super().__init__()
        self.id = None
        self.table = self.get_subclass_name().lower()
        self.lazies = set()
        self.transients = set()
        self.elements = {}
        self._mixin(kwargs)
        self.id_column = "id"
        self.column_names = list(kwargs.keys())
        self.one_to_many = {}
        self.many_to_one = {}
        self.override_methods()

    def _mixin(self, data):
        if isinstance(data, dict):
            self.lazies -= data.keys()
            self.elements = {**self.elements, **data}
        elif isinstance(data, IdEntity):
            self.elements[data.table] = data


    def set_table(self, table_name):
        self.table = table_name

    def set_id_column(self, col_name):
        self.id_column = col_name

    def add_lazy(self, fxn):
        self.lazies.add(fxn[4:])

    def add_transient(self, fxn):
        self.transients.add(fxn)

    def addOneToMany(self, other_table, join_column):
        self.one_to_many[other_table[4:]] = join_column

    def addManyToOne(self, other_table, join_column):
        self.many_to_one[other_table[4:]] = join_column

    def get_id(self):
        return self.id

    def set_id(self, id):
        self.id = id

    def init_lazy(self, data):
        self.id = data[self.id_column]
        del data[self.id_column]
        self.elements = data
        self.column_names = data.keys()

    def get_element_methods(self):
        return sorted(list(self.get_subclass_methods() - self.transients)) # deterministic for testing

    def override_methods(self):
        methods = self.get_element_methods()
        for method in methods:
            if method.startswith("get_"):
                method_target = method[4:]
                getattr(self, method)()
                if method_target in self.lazies and method_target in self.elements:
                    self.lazies.remove(method_target)
                if self.id_column == method_target:
                    setattr(self, method, lambda: self.get_id())
                    continue
                if method in self.transients:
                    continue
                setattr(self, method, lambda t=method_target: self.get_element(t))
                if method_target not in self.elements:
                    self.set_element(method_target, None)
            elif method.startswith("set_"):
                method_target = method[4:]
                getattr(self, method)(None)
                if method in self.transients:
                    continue
                setattr(self, method, lambda value: self.set_element(method_target, value))

    def get_element(self, x):
        if x in self.lazies:
            raise LazyInitializationException(x)
        return self.elements[x]

    def set_element(self, x, value):
        self.elements[x] = value
        self.set_dirty(True)

    def get_insert_query(self):
        names_component = ", ".join(["`" + a + "`" for a in self.column_names])
        elements_component = ", ".join(["%s "] * len(self.column_names))
        return "INSERT INTO {} ({}) VALUES ({})".format(self.table,
                                                        names_component,
                                                        elements_component)
    def get_update_query(self):
        updates = ", ".join(["{} = '{}'".format(key, value) for key, value in self.elements.items()])
        return "UPDATE {} SET {} WHERE id = {}".format(self.table,
                                                       updates,
                                                       self.id)

    def get_initialize_query(self, attribute):
        return "SELECT {} FROM {} WHERE {} = {}".format(attribute, self.table, self.id_column, self.id)

    def get_delete_query(self):
        return "DELETE FROM {} WHERE {} = {}".format(self.table, self.id_column, self.id)

    def get_select_all_query(self):
        return "SELECT * FROM {} WHERE {} = {}".format(self.table, self.id_column, self.id)

    def get_eager_fields(self):
        return self.elements.keys() - self.lazies - self.transients - self.one_to_many.keys() - self.many_to_one.keys()

    def get_many_to_one_relationships(self):
        return self.many_to_one

    def get_one_to_many_relationships(self):
        return self.one_to_many

    def get_select_lazy_query(self):
        fields = self.get_eager_fields()
        fields.add(self.id_column)
        eager_fields = ", ".join(fields)
        return "SELECT {} FROM {} WHERE {} = {}".format(eager_fields, self.table, self.id_column, self.id)

    def get_raw_elements(self):
        return [self.elements[k] for k in self.column_names]

    def get_subclass_name(self):
        return self.__class__.__name__

    def get_subclass_methods(self):
        return {func for func in dir(self.__class__) if callable(getattr(self.__class__, func))} \
               - {func for func in dir(IdEntity) if callable(getattr(IdEntity, func))}
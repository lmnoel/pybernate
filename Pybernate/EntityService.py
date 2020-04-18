from Pybernate.Exceptions import NoMatchingSchemaException, NoSuchEntityException, InvalidEntityServiceException
from cachetools import LFUCache
from collections import Iterable


class SafeExecutor:
    def __init__(self, name):
        self.name = name

    def execute(self, cursor, query, values):
        try:
            cursor.execute(query, values)
        except Exception:
            raise NoMatchingSchemaException(self.name)


class EntityCache(LFUCache):
    def __init__(self, maxsize, connection, clazz, safe_executor):
        super(EntityCache, self).__init__(maxsize)
        self.connection = connection
        self.clazz = clazz
        self.safe_executor = safe_executor

    def get_cursor(self):
        return self.connection.cursor()

    def _by_id(self, id, cursor):
        entity = self.clazz()
        entity.set_id(id)
        try:
            cursor.execute(entity.get_select_lazy_query())
        except Exception:
            raise NoMatchingSchemaException(self.clazz.__name__)
        data = cursor.fetchone()
        if data == None:
            raise NoSuchEntityException(self.clazz.__name__, id)
        entity.init_lazy(data)
        entity.set_initialized(False)
        self.cache(entity.get_id(), entity)
        return entity

    def get_by_id(self, id):
        entity = self.get(id, None)
        if entity is not None:
            if entity.get_deleted():
                raise NoSuchEntityException(self.clazz.__name__, id)
            return entity
        with self.connection.cursor() as cursor:
            return self._by_id(id, cursor)

    def get_by_ids(self, ids): # todo optimize
        entities = []
        with self.connection.cursor() as cursor:
            for id in ids:
                entities.append(self._by_id(id, cursor))
        return entities

    def _set(self, entity, cursor):
        if entity.id is None:
            self.safe_executor.execute(cursor, entity.get_insert_query(), entity.get_raw_elements())
            entity.id = cursor.lastrowid
        else:
            entity.set_dirty(True)
        self.cache(entity.id, entity)

    def set(self, to_set):
        if isinstance(to_set, self.clazz):
            with self.connection.cursor() as cursor:
                self._set(to_set, cursor)
        elif isinstance(to_set, Iterable):
            with self.connection.cursor() as cursor:
                for entity_to_set in to_set:
                    self._set(entity_to_set, cursor)
        else:
            raise InvalidEntityServiceException(to_set.__class__.__name__, self.clazz.__name__)

    def _delete(self, to_delete):
        to_delete.set_deleted(True)
        self.cache(to_delete.get_id(), to_delete)

    def delete(self, to_delete):
        if isinstance(to_delete, self.clazz):
            self._delete(to_delete)
        elif isinstance(to_delete, Iterable):
            [self._delete(entity_to_delete) for entity_to_delete in to_delete]
        else:
            raise InvalidEntityServiceException(to_delete.__class__.__name__, self.clazz.__name__)

    def popitem(self):
        key, entity = super().popitem()
        with self.connection.cursor() as cursor:
            if entity.get_deleted():
                self.safe_executor.execute(cursor, entity.get_delete_query(), ())
            elif entity.get_dirty():
                self.safe_executor.execute(cursor, entity.get_update_query(), ())

    def cache(self, key, value):
        super().__setitem__(key, value)

    # todo: not efficient to call popitem and get a cursor for each element
    def flush(self):
        self.clear()


class EntityService:
    def __init__(self, clazz, connection, session, maxsize):
        self.connection = connection
        self.clazz = clazz
        self.session = session
        self.safe_executor = SafeExecutor(self.get_name())
        self.cache = EntityCache(maxsize, self.connection, self.clazz, self.safe_executor)

    def flush_cache(self):
        self.cache.flush()

    def get_conn(self):
        return self.connection.cursor()

    def get_name(self):
        return self.clazz.__name__

    def save(self, to_save):
        self.cache.set(to_save)

    def delete(self, to_delete):
        self.cache.delete(to_delete)


class IdEntityService(EntityService):
    def by_id(self, entity_id, suppression=None):
        entity = self.cache.get_by_id(entity_id)
        if not entity.get_initialized():
            entity = self._init_one_to_many(entity, suppression)
            entity = self._init_many_to_one(entity, suppression)
            self.cache.set(entity)
        return entity


    def by_ids(self, entity_ids, suppression=None):
            return [self.by_id(entity_id, suppression) for entity_id in entity_ids]

    def _init_many_to_one(self, entity, suppress_attribute):
        for other_class, join_column, foreign_key in entity.get_many_to_one_relationships().values():
            if other_class == suppress_attribute:
                continue
            other_service = self.session.services[other_class]
            this_key = entity.id if join_column is entity.id_column else entity.elements[join_column]
            loaded_entity = other_service.by_id(this_key, entity.table.lower())
            loaded_entity._mixin(entity)
            entity.elements[other_class] = loaded_entity
        return entity

    def _init_one_to_many(self, entity, supress_attribute):
        for other_class, join_column, foreign_key, mapped_by in entity.get_one_to_many_relationships().values():
            if other_class == supress_attribute:
                continue
            other_service = self.session.services[other_class]
            ins = other_service.clazz()
            this_key = entity.id if join_column is entity.id_column else entity.elements[join_column]
            q = "SELECT {} FROM {} WHERE {} = {}".format(ins.id_column, ins.table, foreign_key, this_key)
            with self.get_conn() as cursor:
                cursor.execute(q)
                ids = cursor.fetchall()
            loaded_entities = other_service.by_ids([x[join_column] for x in ids], entity.table.lower())
            [loaded_entity._mixin(entity) for loaded_entity in loaded_entities]
            if mapped_by is not None:
                mapped_entities = {}
                for loaded_entity in loaded_entities:
                    mapped_entities[loaded_entity.elements[mapped_by]] = loaded_entity
                entity.elements[other_class] = mapped_entities
            else:
                entity.elements[other_class] = loaded_entities
        return entity

    def initialize(self, entity, attribute):
        with self.get_conn() as cursor:
            cursor.execute(entity.get_initialize_query(attribute))
            data = cursor.fetchone()
            entity._mixin(data)

    def refresh(self, entity):
        raise NotImplementedError
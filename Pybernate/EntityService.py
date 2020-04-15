from Pybernate.Exceptions import NoMatchingSchemaException, NoSuchEntityException


class EntityService:
    def __init__(self, clazz, connection):
        self.connection = connection
        self.clazz = clazz

    def get_conn(self):
        return self.connection.cursor()

    def safe_execute(self, cursor, query, values):
        try:
            cursor.execute(query, values)
        except Exception:
            raise NoMatchingSchemaException(self.clazz.__name__)

    def _save(self, entity, cursor):
        if entity.id is None:
            self.safe_execute(cursor, entity.get_insert_query(), entity.get_raw_elements())
            entity.id = cursor.lastrowid
            entity.set_persisted()
        else:
            self.safe_execute(cursor, entity.get_update_query(), ())
            entity.set_dirty(False)

    def save(self, to_save):
        if isinstance(to_save, self.clazz):
            with self.get_conn() as cursor:
                self._save(to_save, cursor)
        elif isinstance(to_save, list):
            with self.get_conn() as cursor:
                for entity_to_save in to_save:
                    self._save(entity_to_save, cursor)
        self.connection.commit()

    def delete(self, to_delete):
        if isinstance(to_delete, self.clazz):
            with self.get_conn() as cursor:
                self.safe_execute(cursor, to_delete.get_delete_query(), ())
        elif isinstance(to_delete, list):
            with self.get_conn() as cursor:
                for entity_to_delete in to_delete:
                    self.safe_execute(cursor, entity_to_delete.get_delete_query(), ())
        self.connection.commit()


class IdEntityService(EntityService):
    def _by_id(self, entity_id, cursor):
        entity = self.clazz()
        entity.set_id(entity_id)
        try:
            cursor.execute(entity.get_select_lazy_query())
        except Exception:
            raise NoMatchingSchemaException(self.clazz.__name__)
        data = cursor.fetchone()
        if data == None:
            raise NoSuchEntityException(self.clazz.__name__, entity_id)
        entity.init_lazy(data)
        return entity

    def by_id(self, entity_id):
        with self.get_conn() as cursor:
            return self._by_id(entity_id, cursor)

    def by_ids(self, entity_ids):
        with self.get_conn() as cursor:
            return [self._by_id(entity_id, cursor) for entity_id in entity_ids]

    def initialize(self, entity, fxn_name):
        with self.get_conn() as cursor:
            cursor.execute(entity.get_initialize_query(fxn_name))
            data = cursor.fetchone()
            entity._mixin(data)

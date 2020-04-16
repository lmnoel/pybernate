from Pybernate.Exceptions import NoMatchingSchemaException, NoSuchEntityException

class EntityService:
    def __init__(self, clazz, connection, session):
        self.connection = connection
        self.clazz = clazz
        self.session = session

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
    def _by_id(self, entity_id, cursor, suppress_attribute=None):
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
        entity = self._init_one_to_many(entity, suppress_attribute)
        entity = self._init_many_to_one(entity, suppress_attribute)
        return entity

    def _init_many_to_one(self, entity, suppress_attribute):
        for other_class, join_column, foreign_key in entity.get_many_to_one_relationships().values():
            if other_class == suppress_attribute:
                continue
            other_service = self.session.services[other_class]
            this_key = entity.id if join_column is entity.id_column else entity.elements[join_column]
            loaded_entity = other_service.by_id_with_suppression(this_key, entity.table.lower())
            loaded_entity._mixin(entity)
            entity.elements[other_class] = loaded_entity
        return entity

    def _init_one_to_many(self, entity, supress_attribute):
        for other_class, join_column, foreign_key in entity.get_one_to_many_relationships().values():
            if other_class == supress_attribute:
                continue
            other_service = self.session.services[other_class]
            ins = other_service.clazz()
            this_key = entity.id if join_column is entity.id_column else entity.elements[join_column]
            q = "SELECT {} FROM {} WHERE {} = {}".format(ins.id_column, ins.table, foreign_key, this_key)
            with self.get_conn() as cursor:
                cursor.execute(q)
                ids = cursor.fetchall()
            loaded_entities = other_service.by_ids_with_suppression([x[join_column] for x in ids], entity.table.lower())
            [loaded_entity._mixin(entity) for loaded_entity in loaded_entities]
            entity.elements[other_class] = loaded_entities
        return entity

    def by_id_with_suppression(self, entity_id, suppress_attribute):
        with self.get_conn() as cursor:
            return self._by_id(entity_id, cursor, suppress_attribute)

    def by_id(self, entity_id):
        with self.get_conn() as cursor:
            return self._by_id(entity_id, cursor)

    def by_ids(self, entity_ids):
        with self.get_conn() as cursor:
            return [self._by_id(entity_id, cursor) for entity_id in entity_ids]

    def by_ids_with_suppression(self, entity_ids, suppress_attribute):
        with self.get_conn() as cursor:
            return [self._by_id(entity_id, cursor, suppress_attribute) for entity_id in entity_ids]

    def initialize(self, entity, fxn_name):
        with self.get_conn() as cursor:
            cursor.execute(entity.get_initialize_query(fxn_name))
            data = cursor.fetchone()
            entity._mixin(data)

    def refresh(self, entity):
        raise NotImplementedError
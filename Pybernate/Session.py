from Pybernate.EntityService import IdEntityService
from Pybernate.Exceptions import ServiceAlreadyRegisteredException, NoRegisteredEntityException


# TODO: cache
# TODO: transactional context
class PybernateSession:
    def __init__(self, conn):
        self.conn = conn
        self.services = {}
        self.in_transaction = False

    def register_class(self, *args):
        for clazz in args:
            clazz_name = clazz.__name__
            if clazz_name in self.services:
                raise ServiceAlreadyRegisteredException(clazz_name)
            lower_clazz_name = clazz_name.lower()
            self.services[lower_clazz_name] = IdEntityService(clazz, self.conn, self)

    def get_service(self, service_name):
        if service_name in self.services:
            return self.services[service_name]
        else:
            raise NoRegisteredEntityException(service_name)

    def transactional_commit(self):
        if self.in_transaction:
            return
        else:
            self.conn.commit()

    def begin_transaction(self):
        self.in_transaction = True

    def end_transaction(self):
        self.in_transaction = False
        self.conn.commit()

    def rollback_transaction(self):
        self.in_transaction = False
        self.conn.rollback()

    def transactional(self):
        return TransactionalContext(self)


class TransactionalContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        self.session.begin_transaction()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is None:
            self.session.end_transaction()
            return True
        self.session.rollback_transaction()
        return False

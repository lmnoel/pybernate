from Pybernate.EntityService import IdEntityService
from Pybernate.Exceptions import ServiceAlreadyRegisteredException


# TODO: cache
# TODO: transactional context
class PybernateSession:
    def __init__(self, conn):
        self.conn = conn
        self.services = {}

    def register_class(self, *args):
        for clazz in args:
            clazz_name = clazz.__name__
            if clazz_name in self.services:
                raise ServiceAlreadyRegisteredException(clazz_name)
            lower_clazz_name = clazz_name.lower()
            self.services[lower_clazz_name] = IdEntityService(clazz, self.conn, self)
            service_getter = "get_{}_service".format(lower_clazz_name)
            setattr(self, service_getter, lambda x=lower_clazz_name: self.services[x])
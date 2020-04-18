from Pybernate.EntityService import IdEntityService
from Pybernate.Exceptions import ServiceAlreadyRegisteredException, NoRegisteredEntityException



# TODO: cache
class PybernateSession:
    def __init__(self, conn, maxsize=20):
        self.conn = conn
        self.services = {}
        self.in_transaction = False
        self.maxsize = maxsize

    def register_class(self, *args):
        for clazz in args:
            clazz_name = clazz.__name__
            if clazz_name in self.services:
                raise ServiceAlreadyRegisteredException(clazz_name)
            lower_clazz_name = clazz_name.lower()
            self.services[lower_clazz_name] = IdEntityService(clazz, self.conn, self, self.maxsize)

    def get_service(self, service_name):
        if service_name in self.services:
            return self.services[service_name]
        else:
            raise NoRegisteredEntityException(service_name)

    def end_session(self):
        for service in self.services.values():
            service.flush_cache()
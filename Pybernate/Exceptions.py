class PybernateException(Exception):
    pass


class LazyInitializationException(PybernateException):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "{} is not initialized".format(self.name)


class NoMatchingSchemaException(PybernateException):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "{} has no matching schema".format(self.name)


class NoSuchEntityException(PybernateException):
    def __init__(self, name, id):
        self.name = name
        self.id = id

    def __str__(self):
        return "There is no {} by id {}".format(self.name, self.id)


class ServiceAlreadyRegisteredException(PybernateException):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "There is already a service for {} ".format(self.name)


class NoRegisteredEntityException(PybernateException):
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return "No service is registered for {} ".format(self.name)


class InvalidEntityServiceException(PybernateException):
    def __init__(self, entity_name, service_name):
        self.entity_name = entity_name
        self.service_name = service_name

    def __str__(self):
        return "Attempted to use {} service with entity: {}".format(self.service_name, self.entity_name)
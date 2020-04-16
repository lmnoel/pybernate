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
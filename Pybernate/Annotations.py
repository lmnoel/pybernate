import functools

def lazy(func):
    def func_wrapper(*args, **kwargs):
        args[0].add_lazy(func.__name__)
        return func(*args, **kwargs)

    return func_wrapper


def transient(func):
    def func_wrapper(*args, **kwargs):
        args[0].add_transient(func.__name__)
        return func(*args, **kwargs)

    return func_wrapper

def id(func):
    def func_wrapper(*args, **kwargs):
        args[0].set_id_column(func.__name__[4:])
        return func(*args, **kwargs)

    return func_wrapper


# class table:
#     def __init__(self, **kwargs):
#         self.table = kwargs["name"]
#
#     def __call__(self, fn):
#         @functools.wraps(fn)
#         def decorated(*args, **kwargs):
#             fn.set_table(fn.__name__, self.table)
#             fn(*args, **kwargs)
#         return decorated

class table:
    def __init__(self, **kwargs):
        self.table = kwargs["name"]

    def __call__(self, fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            args[0].set_table(fn.__name__, self.table)
            fn(*args, **kwargs)
        return decorated

# class column:
#     def __init__(self, **kwargs):
#         self.name = kwargs["name"]
#
#     def __call__(self, fn):
#         @functools.wraps(fn)
#         def decorated(*args, **kwargs):
#             args[0].set_column(fn.__name__, self.name)
#             fn(*args, **kwargs)
#         return decorated

class oneToMany:
    def __init__(self, **kwargs):
        self.join_table = kwargs["join_table"]
        self.join_column = kwargs["join_column"]

    def __call__(self, fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            args[0].addOneToMany(fn.__name__, (self.join_table, self.join_column))
            fn(*args, **kwargs)
        return decorated


class manyToOne:
    def __init__(self, **kwargs):
        self.join_table = kwargs["join_table"]
        self.join_column = kwargs["join_column"]

    def __call__(self, fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            args[0].addManyToOne(fn.__name__, (self.join_table, self.join_column))
            fn(*args, **kwargs)
        return decorated
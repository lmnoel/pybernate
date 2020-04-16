import unittest
import pymysql.cursors
from Pybernate.EntityService import IdEntityService
from Pybernate.Entity import IdEntity
from Pybernate.Annotations import lazy, transient, id, oneToMany, manyToOne
from Pybernate.Exceptions import LazyInitializationException, NoMatchingSchemaException, NoSuchEntityException
from Pybernate.Session import PybernateSession


class Foo(IdEntity):
    def get_a(self):
        return

    def set_a(self, val):
        return

    def get_b(self):
        return

    def set_b(self, val):
        return

class Bar(IdEntity):
    @id
    def get_not_id(self):
        return

    def get_c(self):
        return

    def set_c(self, val):
        return

    @lazy
    def get_d(self):
        return

    @transient
    def get_other(self):
        return 5


class Baz(IdEntity):
    def get_a(self):
        return


class Zoo(IdEntity):

    def get_c(self):
        return

    def set_c(self, val):
        return



class Fez(IdEntity):

    def get_a(self):
        return

    @oneToMany(join_column="id", join_table="bez", foreign_key="foreign_id")
    def get_bez(self):
        return

    def add_bez(self, bez): # also update
        return

    def delete_bez(self):
        return

class Bez(IdEntity):
    @manyToOne(join_column="foreign_id", join_table="fez", foreign_key="id")
    def get_fez(self):
        return

    def get_foreign_id(self):
        return

class Test(unittest.TestCase):

    def setUp(self):
        self.connection = pymysql.connect(host='',
                                     user='local',
                                     password='',
                                     db='Pybernate',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        self.session = PybernateSession(self.connection)
        self.session.register_class(Foo, Bar, Baz, Fez, Bez)
        self.foo_service = self.session.get_foo_service()
        self.bar_service = self.session.get_bar_service()
        self.baz_service = self.session.get_baz_service()
        self.fez_service = self.session.get_fez_service()
        self.bez_service = self.session.get_bez_service()
        with self.connection.cursor() as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS foo (a int, b VARCHAR(10), id INT AUTO_INCREMENT KEY)")
            cursor.execute("CREATE TABLE IF NOT EXISTS bar (c INT, d VARCHAR(10), not_id INT AUTO_INCREMENT KEY)")
            cursor.execute("CREATE TABLE IF NOT EXISTS fez (a INT, id INT AUTO_INCREMENT KEY)")
            cursor.execute("CREATE TABLE IF NOT EXISTS bez (foreign_id INT, id INT AUTO_INCREMENT KEY)")
        self.connection.commit()

    def tearDown(self):
        with self.connection.cursor() as cursor:
            cursor.execute("DROP TABLE foo")
            cursor.execute("DROP TABLE bar")
            cursor.execute("DROP TABLE fez")
            cursor.execute("DROP TABLE bez")
        self.connection.commit()

    def test_save_load(self):
        foo = Foo(a=1, b="two")
        self.foo_service.save(foo)
        foo_too = self.foo_service.by_id(foo.id)
        foo_three = Foo(a=2, b="four")
        assert foo.get_a() == foo_too.get_a() == 1
        assert foo.get_b() == foo_too.get_b() == "two"
        assert foo_three.get_a() == 2
        assert foo_three.get_b() == "four"

    def test_set(self):
        foo = Foo(a=1, b="two")
        foo.set_b("three")
        self.foo_service.save(foo)
        foo_too = self.foo_service.by_id(foo.id)
        assert foo.get_b() == foo_too.get_b() == "three"
        foo.set_b("four")
        self.foo_service.save(foo)
        foo_three = self.foo_service.by_id(foo.id)
        assert foo.get_b() == foo_three.get_b() == "four"

    def test_lazy_initialization(self):
        bar = Bar(c=2, d="three")
        self.bar_service.save(bar)
        bar_too = self.bar_service.by_id(bar.id)
        try:
            bar_too.get_d()
            assert False
        except LazyInitializationException:
            pass
        self.bar_service.initialize(bar_too, "d")
        assert bar.get_d() == bar_too.get_d() == "three"

    def test_transient(self):
        bar = Bar(c=2, d="three")
        assert bar.get_other() == 5

    def test_no_matching_schema_exception(self):
        baz = Baz(a=2)
        try:
            self.baz_service.save(baz)
            assert False
        except NoMatchingSchemaException:
            pass

    def test_delete(self):
        foo = Foo(a=1, b="three")
        self.foo_service.save(foo)
        self.foo_service.delete(foo)
        try:
            foo_too = self.foo_service.by_id(foo.id)
            assert False
        except NoSuchEntityException:
            pass

    def test_one_to_many(self):
        fez = Fez(a=1)
        self.fez_service.save(fez)
        bez_0 = Bez(foreign_id=fez.id)
        bez_1 = Bez(foreign_id=fez.id)
        self.bez_service.save([bez_0, bez_1])

        fez_too = self.fez_service.by_id(fez.id)
        bezzes = fez_too.get_bez()
        assert len(bezzes) == 2
        for bez in bezzes:
            assert bez.get_foreign_id() == fez.get_id()

    def tet_many_to_one(self):

        assert False
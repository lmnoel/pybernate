import unittest
import pymysql.cursors
from Pybernate.Entity import IdEntity
from Pybernate.Annotations import lazy, transient, id, oneToMany, manyToOne
from Pybernate.Exceptions import LazyInitializationException, NoMatchingSchemaException, NoSuchEntityException, InvalidEntityServiceException
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

    def add_bez(self, bez):
        return

    @oneToMany(join_column="id", join_table="pez", foreign_key="foreign_id", mapped_by="color")
    def get_pez(self):
        return

    def delete_bez(self):
        return

class Bez(IdEntity):
    @manyToOne(join_column="foreign_id", join_table="fez", foreign_key="id")
    def get_fez(self):
        return

    def get_foreign_id(self):
        return


class Pez(IdEntity):
    def get_foreign_id(self):
        return

    def get_color(self):
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
        self.session.register_class(Foo, Bar, Baz, Fez, Bez, Pez)
        self.foo_service = self.session.get_service("foo")
        self.bar_service = self.session.get_service("bar")
        self.baz_service = self.session.get_service("baz")
        self.fez_service = self.session.get_service("fez")
        self.bez_service = self.session.get_service("bez")
        self.pez_service = self.session.get_service("pez")
        with self.connection.cursor() as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS foo (a int, b VARCHAR(10), id INT AUTO_INCREMENT KEY)")
            cursor.execute("CREATE TABLE IF NOT EXISTS bar (c INT, d VARCHAR(10), not_id INT AUTO_INCREMENT KEY)")
            cursor.execute("CREATE TABLE IF NOT EXISTS fez (a INT, id INT AUTO_INCREMENT KEY)")
            cursor.execute("CREATE TABLE IF NOT EXISTS bez (foreign_id INT, id INT AUTO_INCREMENT KEY)")
            cursor.execute("CREATE TABLE IF NOT EXISTS pez (foreign_id INT, color VARCHAR(10), id INT AUTO_INCREMENT KEY)")
        self.connection.commit()

    def tearDown(self):
        with self.connection.cursor() as cursor:
            cursor.execute("DROP TABLE foo")
            cursor.execute("DROP TABLE bar")
            cursor.execute("DROP TABLE fez")
            cursor.execute("DROP TABLE bez")
            cursor.execute("DROP TABLE pez")
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
            assert bez.get_fez().get_a() == fez.get_a() == 1

    def test_mapped_by(self):
        fez = Fez(a=2)
        self.fez_service.save(fez)
        pez_0 = Pez(foreign_id=fez.id, color="blue")
        pez_1 = Pez(foreign_id=fez.id, color="green")
        pez_2 = Pez(foreign_id=fez.id, color="yellow")
        self.pez_service.save([pez_0, pez_1, pez_2])
        fez_too = self.fez_service.by_id(fez.get_id())
        pezzes = fez_too.get_pez()
        assert isinstance(pezzes, dict)
        assert len(pezzes) == 3
        for color in ["blue", "green", "yellow"]:
            mapped_pez = pezzes[color]
            assert mapped_pez.get_color() == color and mapped_pez.get_foreign_id() == fez.get_id()

    def test_wrong_entity_for_service(self):
        fez = Fez(a=2)
        try:
            self.bez_service.save(fez)
            assert False
        except InvalidEntityServiceException:
            pass
        self.fez_service.save(fez)
        try:
            self.bez_service.delete(fez)
            assert False
        except InvalidEntityServiceException:
            pass

    def test_transactional_context(self):
        fez_0 = Fez(a=0)
        fez_1 = Fez(a=1)
        fez_2 = Fez(a=2)

        # rollback works in context
        try:
            with self.session.transactional():
                self.fez_service.save(fez_0)
                raise RuntimeError()
        except RuntimeError:
            pass
        assert self._check_entity_count("fez", "id", fez_0.id) == 0

        # rollback works not in context
        self.session.begin_transaction()
        self.fez_service.save(fez_1)
        self.session.rollback_transaction()
        assert self._check_entity_count("fez", "id", fez_1.id) == 0

        # entities are saved when no rollback/exceptions
        with self.session.transactional():
            self.fez_service.save(fez_2)

        assert self._check_entity_count("fez", "id", fez_2.id) == 1


    def _check_entity_count(self, entity_name, id_column, id):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM {} WHERE {} = {}".format(entity_name, id_column, id))
            data = cursor.fetchall()
            return len(data)
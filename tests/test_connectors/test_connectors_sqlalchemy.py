"""
Created on 22 Jan 2020

@author: si
"""
import os
import shutil
import tempfile
import unittest

from sqlalchemy import inspect, Column, Integer, String
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base

import ayeaye
from ayeaye.connectors.sqlalchemy_database import SqlAlchemyDatabaseConnector


def fruit_schemas(declarative_base):
    class Pear(declarative_base):
        __tablename__ = "pear"
        id = Column(Integer, primary_key=True)
        variety = Column(String(250), nullable=False)

    class Bananna(declarative_base):
        __tablename__ = "bananna"
        id = Column(Integer, primary_key=True)
        variety = Column(String(250), nullable=False)

    return [Pear, Bananna]


def people_schema(declarative_base):
    class Person(declarative_base):
        __tablename__ = "person"
        id = Column(Integer, primary_key=True)
        surname = Column(String(250), nullable=False)

    return Person


class TestSqlAlchemyConnector(unittest.TestCase):
    def setUp(self):
        self._working_directory = None

    def tearDown(self):
        if self._working_directory and os.path.isdir(self._working_directory):
            shutil.rmtree(self._working_directory)

    def working_directory(self):
        self._working_directory = tempfile.mkdtemp()
        return self._working_directory

    def test_schema_access_multiple(self):
        """
        list of ORM models in schema
        """
        c = SqlAlchemyDatabaseConnector(
            engine_url="sqlite://",
            schema_builder=fruit_schemas,
        )
        # method resolution order
        super_classes = [cc.__name__ for cc in c.schema.Pear.mro()]
        expected = ["Pear", "Base", "object"]  # class above, declarative base, py obj
        self.assertEqual(expected, super_classes)

    def test_schema_access_single(self):
        """
        One ORM model in schema has different way of accessing with .schema
        """
        c = SqlAlchemyDatabaseConnector(
            engine_url="sqlite://",
            schema_builder=people_schema,
        )
        # method resolution order
        super_classes = [cc.__name__ for cc in c.schema.mro()]
        expected = ["Person", "Base", "object"]  # class above, declarative base, py obj
        self.assertEqual(expected, super_classes)

    def test_create_db_schema(self):
        c = SqlAlchemyDatabaseConnector(
            engine_url="sqlite://",
            schema_builder=fruit_schemas,
            access=ayeaye.AccessMode.WRITE,
        )

        c.connect()

        # check there aren't any tables in the DB here
        with self.assertRaises(OperationalError) as context:
            c.session.query(c.schema.Pear).all()

        self.assertIn("no such table: pear", str(context.exception))

        c.create_table_schema()

        # but there are tables now (but no data in them)
        all_the_pears = c.session.query(c.schema.Pear).all()
        self.assertIsInstance(all_the_pears, list)
        self.assertEqual(0, len(all_the_pears))

    def test_add_orm_data_single(self):
        c = SqlAlchemyDatabaseConnector(
            engine_url="sqlite://", schema_builder=people_schema, access=ayeaye.AccessMode.READWRITE
        )
        c.create_table_schema()

        c.add({"surname": "Cornwallis"})
        c.add({"surname": "Brunel"})
        c.commit()

        # read them back
        all_the_people = [f"{p.id} {p.surname}" for p in c]
        expected = "1 Cornwallis 2 Brunel"
        self.assertEqual(expected, " ".join(all_the_people))

    def test_add_orm_data_multiple(self):
        c = SqlAlchemyDatabaseConnector(
            engine_url="sqlite://", schema_builder=fruit_schemas, access=ayeaye.AccessMode.READWRITE
        )
        c.connect()
        c.create_table_schema()

        with self.assertRaises(ValueError) as context:
            c.add({"variety": "Cavendish"})

        self.assertIn("Dictionary can only be used in single schema mode", str(context.exception))

        c.add(c.schema.Pear(variety="D'Anjou"))
        c.add(c.schema.Bananna(variety="Cavendish"))
        c.commit()

        # read back mixed types with primary key values belonging to each table (i.e. both are 1)
        mixed_records = [r.__tablename__ + str(r.id) for r in c]
        expected = "pear1 bananna1"
        self.assertEqual(expected, " ".join(mixed_records))

    def test_on_disk(self):
        """
        All the other tests are in memory. Ensure to disk works.

        This test is also being created because windows is refusing to delete open files so
        confirmation that close_connection() is working was experimented with using lsof under
        OSX. But the file handle isn't left open so can't be part of this test.
        """
        db_file = "{}/fruit.db".format(self.working_directory())
        c = SqlAlchemyDatabaseConnector(
            engine_url=f"sqlite:///{db_file}",
            schema_builder=fruit_schemas,
            access=ayeaye.AccessMode.READWRITE,
        )
        c.connect()
        c.create_table_schema()

        c.add(c.schema.Pear(variety="Comice"))
        c.commit()

        c.close_connection()

        self.assertTrue(os.access(db_file, os.R_OK))

    def test_double_close_sqlite(self):
        """
        TODO - can't reproduce the "Cannot operate on a closed database." Sqlite error.
        """
        db_file = "{}/fruit.db".format(self.working_directory())
        c = SqlAlchemyDatabaseConnector(
            engine_url=f"sqlite:///{db_file}",
            schema_builder=fruit_schemas,
            access=ayeaye.AccessMode.READWRITE,
        )
        c.connect()
        c.create_table_schema()

        c.add(c.schema.Pear(variety="Williams"))
        # c.commit()

        c.close_connection()
        c.__del__()

    def test_two_databases(self):
        """
        The declarative base is created by each SqlAlchemyDatabaseConnector. Ensure models passed
        to each Connector stay in their own engines.
        """
        db_file = "{}/fruit.db".format(self.working_directory())
        fruit = SqlAlchemyDatabaseConnector(
            engine_url=f"sqlite:///{db_file}",
            schema_builder=fruit_schemas,
            access=ayeaye.AccessMode.READWRITE,
        )
        fruit.create_table_schema()

        db_file = "{}/people.db".format(self.working_directory())
        people = SqlAlchemyDatabaseConnector(
            engine_url=f"sqlite:///{db_file}",
            schema_builder=people_schema,
            access=ayeaye.AccessMode.READWRITE,
        )
        people.create_table_schema()

        # Tables creates in correct DB
        # ----------------------
        inspector = inspect(fruit.engine)
        fruit_tables = {table_name for table_name in inspector.get_table_names()}
        self.assertEqual({"bananna", "pear"}, fruit_tables)

        inspector = inspect(people.engine)
        people_tables = {table_name for table_name in inspector.get_table_names()}
        self.assertEqual({"person"}, people_tables)

        # Tables can be used in the normal way
        # ----------------------
        fruit.add(fruit.schema.Pear(variety="Comice"))
        fruit.commit()
        fruit.close_connection()

        people.add({"surname": "Attenborough"})
        people.commit()
        people.close_connection()

    def test_schema_builder_model_exclusive(self):
        PeoplCls = people_schema(declarative_base=declarative_base())

        with self.assertRaises(ValueError):
            SqlAlchemyDatabaseConnector(
                engine_url="sqlite:////tmp/wontbecreated.db",
                schema_builder=people_schema,
                schema_model=PeoplCls,
                access=ayeaye.AccessMode.READWRITE,
            )

    def test_schema_model_single(self):
        """
        Instead of passing a callable (i.e. schema_builder argument) pass an SqlAlchemy model
        which already has a declarative base.
        """
        Base = declarative_base()

        class Rodents(Base):
            __tablename__ = "rodent"
            id = Column(Integer, primary_key=True)
            species = Column(String(250), nullable=False)

        db_file = "{}/rodents.db".format(self.working_directory())
        rodents = SqlAlchemyDatabaseConnector(
            engine_url=f"sqlite:///{db_file}",
            schema_model=Rodents,
            access=ayeaye.AccessMode.READWRITE,
        )
        rodents.create_table_schema()
        rodents.add({"species": "Yellow-necked mouse"})
        rodents.commit()

        # should also be possible to pass an ORM instance
        msg = "The id will be populated after the commit"
        black_rat = Rodents(species="Black rat")
        self.assertIsNone(black_rat.id, msg)

        rodents.add(black_rat)
        rodents.commit()

        self.assertIsInstance(black_rat.id, int, msg)

        rodents.close_connection()

    def test_schema_model_multiple(self):
        """
        see :method:`` but with a list, same idea as how the schema_builder argument can return a
        single schema or list.
        """
        Base = declarative_base()

        class Cats(Base):
            __tablename__ = "cat"
            id = Column(Integer, primary_key=True)
            name = Column(String(250))

        class Dogs(Base):
            __tablename__ = "dog"
            id = Column(Integer, primary_key=True)
            name = Column(String(250))

        db_file = "{}/pets.db".format(self.working_directory())
        pets = SqlAlchemyDatabaseConnector(
            engine_url=f"sqlite:///{db_file}",
            schema_model=[Cats, Dogs],
            access=ayeaye.AccessMode.READWRITE,
        )
        pets.create_table_schema()

        with self.assertRaises(ValueError) as context:
            pets.add({"name": "Lady"})
        self.assertIn("Dictionary can only be used in single schema mode", str(context.exception))

        pets.add(pets.schema.Cats(name="Lady"))
        pets.add(pets.schema.Dogs(name="Lady"))

        pets.commit()
        pets.close_connection()

    def test_schema_model_multiple_bases(self):
        """
        Multiple declarative bases on same Connector should fail.
        """
        BaseX = declarative_base()
        BaseY = declarative_base()

        class Cats(BaseX):
            __tablename__ = "cat"
            id = Column(Integer, primary_key=True)
            name = Column(String(250))

        class Dogs(BaseY):
            __tablename__ = "dog"
            id = Column(Integer, primary_key=True)
            name = Column(String(250))

        db_file = "{}/pets.db".format(self.working_directory())
        c = SqlAlchemyDatabaseConnector(
            engine_url=f"sqlite:///{db_file}",
            schema_model=[Cats, Dogs],
            access=ayeaye.AccessMode.READWRITE,
        )
        with self.assertRaises(ValueError) as context:
            c.connect()

        self.assertIn(
            "Models passed to `schema_model` must share the same declarative base",
            str(context.exception),
        )

    def test_sql_direct(self):
        """
        SQL queries without SqlAlchemy ORM models.
        """
        c = SqlAlchemyDatabaseConnector(engine_url="sqlite://")
        c.sql("CREATE TABLE nice_colours (colour varchar(20))")
        c.sql("INSERT INTO nice_colours values ('blue'), ('green'), ('black')")
        results = c.sql(
            "SELECT colour FROM nice_colours where colour <> :not_really_a_colour",
            not_really_a_colour="black",
        )
        final_colours = set()
        for r in results:
            final_colours.add(r._mapping["colour"])

        assert set(["blue", "green"]) == final_colours

    def test_simple_query(self):
        """
        SqlAlchemy ORM query.
        """
        Base = declarative_base()

        class Moths(Base):
            __tablename__ = "rodent"
            id = Column(Integer, primary_key=True)
            common_name = Column(String(250))
            scientific_name = Column(String(250))
            notes = Column(String(250))

        db_file = "{}/moths.db".format(self.working_directory())
        moths = SqlAlchemyDatabaseConnector(
            engine_url=f"sqlite:///{db_file}",
            schema_model=Moths,
            access=ayeaye.AccessMode.READWRITE,
        )
        moths.create_table_schema()

        for common_name, scientific_name, notes in [
            ("Atlas moth", "Attacus atlas", "one of the largest moths in the world"),
            ("Herculese moth", "Coscinocera hercules", "largest moth in Australia"),
            ("White witch moth", "Thysania agrippina", "long longest wingspan"),
            ("Madagascan sunset moth", "Chrysiridia rhipheus", "v. impressive and beautiful"),
        ]:
            r = dict(common_name=common_name, scientific_name=scientific_name, notes=notes)
            moths.add(r)

        moths.commit()

        atlas_moth = moths.query.filter_by(common_name="Atlas moth").one()
        self.assertEqual("Attacus atlas", atlas_moth.scientific_name)

        super_moths = moths.query.filter(moths.schema.notes.like("%largest%")).all()
        super_moth_names = set([m.common_name for m in super_moths])
        expected_super_moths = set(["Atlas moth", "Herculese moth"])
        msg = "These have the word largest in the notes field"
        self.assertEqual(expected_super_moths, super_moth_names, msg)

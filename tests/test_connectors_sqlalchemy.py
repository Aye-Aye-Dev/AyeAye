'''
Created on 22 Jan 2020

@author: si
'''
import os
import shutil
import subprocess
import tempfile
import unittest

from sqlalchemy import Column, Integer, String
from sqlalchemy.exc import OperationalError

import ayeaye
from ayeaye.connectors.sqlalchemy_database import SqlAlchemyDatabaseConnector


def fruit_schemas(declarative_base):
    class Pear(declarative_base):
        __tablename__ = 'pear'
        id = Column(Integer, primary_key=True)
        variety = Column(String(250), nullable=False)

    class Bananna(declarative_base):
        __tablename__ = 'bananna'
        id = Column(Integer, primary_key=True)
        variety = Column(String(250), nullable=False)

    return [Pear, Bananna]


def people_schema(declarative_base):
    class Person(declarative_base):
        __tablename__ = 'person'
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
        c = SqlAlchemyDatabaseConnector(engine_url="sqlite://",
                                        schema_builder=fruit_schemas,
                                        )
        # method resolution order
        super_classes = [cc.__name__ for cc in c.schema.Pear.mro()]
        expected = ['Pear', 'Base', 'object']  # class above, declarative base, py obj
        self.assertEqual(expected, super_classes)

    def test_schema_access_single(self):
        """
        One ORM model in schema has different way of accessing with .schema
        """
        c = SqlAlchemyDatabaseConnector(engine_url="sqlite://",
                                        schema_builder=people_schema,
                                        )
        # method resolution order
        super_classes = [cc.__name__ for cc in c.schema.mro()]
        expected = ['Person', 'Base', 'object']  # class above, declarative base, py obj
        self.assertEqual(expected, super_classes)

    def test_create_db_schema(self):

        c = SqlAlchemyDatabaseConnector(engine_url="sqlite://",
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

        c = SqlAlchemyDatabaseConnector(engine_url="sqlite://",
                                        schema_builder=people_schema,
                                        access=ayeaye.AccessMode.READWRITE
                                        )
        c.create_table_schema()

        c.add({'surname': 'Cornwallis'})
        c.add({'surname': 'Brunel'})
        c.commit()

        # read them back
        all_the_people = [f"{p.id} {p.surname}" for p in c]
        expected = "1 Cornwallis 2 Brunel"
        self.assertEqual(expected, " ".join(all_the_people))

    def test_add_orm_data_multiple(self):

        c = SqlAlchemyDatabaseConnector(engine_url="sqlite://",
                                        schema_builder=fruit_schemas,
                                        access=ayeaye.AccessMode.READWRITE
                                        )
        c.connect()
        c.create_table_schema()

        with self.assertRaises(ValueError) as context:
            c.add({'variety': 'Cavendish'})

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
        c = SqlAlchemyDatabaseConnector(engine_url=f"sqlite:///{db_file}",
                                        schema_builder=fruit_schemas,
                                        access=ayeaye.AccessMode.READWRITE
                                        )
        c.connect()
        c.create_table_schema()

        c.add(c.schema.Pear(variety="Comice"))
        c.commit()

        c.close_connection()

        self.assertTrue(os.access(db_file, os.R_OK))

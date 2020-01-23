'''
Created on 22 Jan 2020

@author: si
'''
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

    def test_schema_access_multiple(self):
        """
        list of ORM models in schema
        """
        c = SqlAlchemyDatabaseConnector(engine_url="sqlite://",
                                        schema_builder=fruit_schemas,
                                        )
        # method resolution order
        super_classes = [cc.__name__ for cc in c.schema.Pear.mro()]
        expected = ['Pear', 'Base', 'object'] # class above, declarative base, py obj
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
        expected = ['Person', 'Base', 'object'] # class above, declarative base, py obj
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
        mixed_records = [r.__tablename__+str(r.id) for r in c]
        expected = "pear1 bananna1"
        self.assertEqual(expected, " ".join(mixed_records))

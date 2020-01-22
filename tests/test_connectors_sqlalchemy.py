'''
Created on 22 Jan 2020

@author: si
'''
import unittest

from sqlalchemy import Column, Integer, String

from ayeaye.connectors.sqlalchemy_database import SqlAlchemyDatabaseConnector

def fruit_schemas(declarative_base):
    class Pears(declarative_base):
        __tablename__ = 'pears'
        id = Column(Integer, primary_key=True)
        name = Column(String(250), nullable=False)

    return [Pears]

class TestSqlAlchemyConnector(unittest.TestCase):

    def setUp(self):
        self.temp_sqlite_url = "sqlite://"

    def test_schema_access(self):

        c = SqlAlchemyDatabaseConnector(engine_url=self.temp_sqlite_url,
                                        schema_builder=fruit_schemas,
                                        )
        # method resolution order
        super_classes = [cc.__name__ for cc in c.schema.Pears.mro()]
        expected = ['Pears', 'Base', 'object'] # class above, declarative base, py obj
        self.assertEqual(expected, super_classes)

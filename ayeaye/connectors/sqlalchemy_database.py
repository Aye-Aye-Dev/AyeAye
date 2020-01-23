'''
Created on 22 Jan 2020

@author: si
'''
try:
    from sqlalchemy import create_engine
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
except:
    pass

from ayeaye.connectors.base import DataConnector, AccessMode
from ayeaye.pinnate import Pinnate


class SqlAlchemyDatabaseConnector(DataConnector):
    engine_type = ['sqlite://', 'mysql://']
    optional_args = {'schema_builder': None}
    # TODO implement mysql

    def __init__(self, *args, **kwargs):
        """
        Connector to relational databases supported by SQLAlchemy.
        https://www.sqlalchemy.org/

        Supports SQLAlemy's ORM (Object Relational Mapper)

        For args: @see :class:`connectors.base.DataConnector`

        additional args for SqlalchemyDatabaseConnector
            schema_builder (optional) (callable) taking declarative base as the single argument.
            Must return a list of classes or single class with the shared declarative base.
            see https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/api.html

            The behaviour of this connection will differ on single or list being passed.
            For example:
              .schema requires the name of the class in multiple mode.
              e.g.  my_connection.schema.OrmClass (for multiple mode)
                    and my_connection.schema (for single mode)

        Connection information-
            engine_url format varied with each engine
        e.g. sqlite:////data/sensors.db  for SQLite DB stored in '/data/sensors.db'
        """
        super().__init__(*args, **kwargs)
        
        # on demand see :method:`connect`
        # the engine, declarative base class and sessions belong to this connection. Sharing these
        # between connections within a single model is not yet implemented.
        self.Base = None # the declarative base
        self.session = None
        self.engine = None
        
        # self.schema_builder is built by init from the optional args
        self._schema_p = None # see :method:`schema`

    def connect(self):

        if self.Base is None:
            self.Base = declarative_base()
            self.engine = create_engine(self.engine_url)

            # Bind the engine to the metadata of the Base class so that the
            # declaratives can be accessed through a DBSession instance
            self.Base.metadata.bind = self.engine

            DBSession = sessionmaker(bind=self.engine)
            # A DBSession() instance establishes all conversations with the database
            # and represents a "staging zone" for all the objects loaded into the
            # database session object. Any change made against the objects in the
            # session won't be persisted into the database until you call
            # session.commit(). If you're not happy about the changes, you can
            # revert all of them back to the last commit by calling
            # session.rollback()
            self.session = DBSession()

            # initialise schema            
            schema_classes = self.schema_builder(self.Base) if self.schema_builder is not None else []
            if isinstance(schema_classes, list):
                as_dict = {c.__name__: c for c in schema_classes}
                self._schema_p = Pinnate(as_dict)
            else:
                self._schema_p = schema_classes # single class

    def create_table_schema(self):
        """
        Create the tables defined in self.schema
        """
        if self.access == AccessMode.READ:
            raise ValueError("Can not build schema when access == READ")

        self.connect()
        self.Base.metadata.create_all(self.engine)

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        raise NotImplementedError("TODO")

    @property
    def data(self):
        raise NotImplementedError("TODO")

    @property
    def schema(self):
        """
        SQLAlchemy's ORM classes represent tables in the underlying database.

        This property might be pushed up to :class:`DataConnector` so all Connectors could implement schemas.

        :returns: instance of :class:`AyeAye.pinnate` with names of the ORM classes as keys and the
        class (not instance) as the value.
        """
        self.connect()
        return self._schema_p

'''
Created on 22 Jan 2020

@author: si
'''
try:
    from sqlalchemy import create_engine
    from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.sql import text
except ModuleNotFoundError:
    pass

from ayeaye.connectors.base import DataConnector, AccessMode
from ayeaye.pinnate import Pinnate


class SqlAlchemyDatabaseConnector(DataConnector):
    engine_type = ['sqlite://', 'mysql://', 'mysql+pymysql://', 'postgresql://']
    optional_args = {'schema_builder': None,
                     'schema_model': None,
                     }

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
                    and my_connection.schema (for 'single schema mode')

            Can't be passed alongside `schema_model`.

            schema_model (optional) SqlAlchemy Model (subclass of
                function:`sqlalchemy.ext.declarative.declarative_base`). Can be passed along-side
                `schema_builder`.

        Connection information-
            engine_url format varied with each engine
        e.g. sqlite:////data/sensors.db  for SQLite DB stored in '/data/sensors.db'
        """
        super().__init__(*args, **kwargs)

        if (self.schema_builder is not None) and (self.schema_model is not None):
            msg = "`schema_builder` and `schema_model` arguments are mutually exclusive"
            raise ValueError(msg)

        # on demand see :method:`connect`
        # the engine, declarative base class and sessions belong to this connection. Sharing these
        # between connections within a single model is not yet implemented.
        self.Base = None  # the declarative base
        self.session = None
        self.engine = None

        # self.schema_builder is built by init from the optional args
        self._schema_p = None  # see :method:`connect`

    def connect(self):

        if self.Base is not None:
            return

        if self.schema_model is not None:

            if isinstance(self.schema_model, list):
                check_schema_models = self.schema_model
            else:
                check_schema_models = [self.schema_model]

            if len(check_schema_models) == 0:
                raise TypeError("No models passed")

            def get_declarative_base(schema_model):
                """Given a class that inherits from SqlAlchemy's declarative base/meta;
                return the class
                """
                if not isinstance(schema_model, DeclarativeMeta):
                    raise TypeError("Not an SqlAlchemy database model")

                # Using method resolution order, find the declarative_base that would be common to
                # all models.
                # e.g. in the following, find Base given MyModel
                #   Base = declarative_base()
                #   ...
                #   class MyModel(Base):
                #       ...
                #
                # in single schema_model mode I'm not sure if common ancestor matters
                resolution_order = schema_model.mro()
                resolution_order.reverse()
                for m in resolution_order:
                    if isinstance(m, DeclarativeMeta):
                        return m
                else:
                    raise ValueError("DeclarativeMeta not found in schema_model??")

            bases = [get_declarative_base(s) for s in check_schema_models]
            for idx in range(len(bases)):
                if idx == 0:
                    continue
                if bases[0] != bases[idx]:
                    msg = "Models passed to `schema_model` must share the same declarative base"
                    raise ValueError(msg)

            self.Base = bases[0]
            self.engine = create_engine(self.engine_url)
            self.Base.metadata.bind = self.engine
            DBSession = sessionmaker(bind=self.engine)
            self.session = DBSession()

            if isinstance(self.schema_model, list):
                as_dict = {c.__name__: c for c in self.schema_model}
                self._schema_p = Pinnate(as_dict)
            else:
                self._schema_p = self.schema_model  # single class

        else:
            # SQL direct or with self.schema_builder callable

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

            if self.schema_builder is not None:
                # initialise schema
                schema_classes = self.schema_builder(self.Base) \
                    if self.schema_builder is not None else []

                if isinstance(schema_classes, list):
                    as_dict = {c.__name__: c for c in schema_classes}
                    self._schema_p = Pinnate(as_dict)
                else:
                    self._schema_p = schema_classes  # single class

    def close_connection(self):
        if self.session is not None:
            self.session.close()

    def __del__(self):
        """
        SqlAlchemy does it's own deconstruction.
        """
        pass

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
        """
        Generator for all records in all schema.
        """
        if self.access not in [AccessMode.READ, AccessMode.READWRITE]:
            raise ValueError("Can not read data without access == READ")

        schemata = [self.schema] if self.is_single_schema_mode else self.schema.values()
        for schema in schemata:
            # TODO take primary key from schema or default to 'id'
            for r in self.session.query(schema).order_by(schema.id).all():
                yield r

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

    @property
    def is_single_schema_mode(self):
        return not isinstance(self.schema, Pinnate)

    def add(self, item):
        """
        @param item: (dict or ORM instance) - dict only with 'single schema mode'
        """
        self.connect()
        if isinstance(item, dict):
            if not self.is_single_schema_mode:
                raise ValueError("Dictionary can only be used in single schema mode")
            item = self.schema(**item)
        else:
            if not isinstance(item, tuple(self.schema.values())):
                msg = "Item of type {} isn't part of this connection's schema"
                raise ValueError(msg.format(type(item)))

        self.session.add(item)

    def commit(self):
        """
        Send pending data changes to the database.
        """
        # only actually needed if no items were added. otherwise the session is already there.
        self.connect()

        # TODO auto commit
        self.session.commit()

    def sql(self, sql_stmt, **sql_params):
        """
        Execute an SQL query against the database engine directly without using the ORM.
        Parameters in the statement are named and the values are passed in a dictionary. Numbered
        positions aren't supported.

        Example- also converts to dictionary
            results = my_db_connector.sql("SELECT * from nice_colours where colour <> :not_really_a_colour",
                                          not_really_a_colour='black'
                                         )
            for r in results:
              print(dict(r))


        @param sql_stmt: (str) SQL statement with
        @param sql_params: (key value pairs), keys must be strings of parameters in SQL statement
        @return: (named tuples) if the query returns results.
        """
        self.connect()
        sql = text(sql_stmt)
        return self.session.execute(sql, sql_params)

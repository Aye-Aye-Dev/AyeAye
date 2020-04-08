class ConnectorResolver:
    """
    Lazy substitution of parameters within engine_urls.

    Normal usage of this module is for a single global instance to be available to all instances of
    all the subclasses of :class:`DataConnector`. This is provided as-

    >>> from ayeaye.connect_resolve import connector_resolver

    This instance is given `resolver_callable`s at runtime and these are later used at runtime when a
    DataConnector uses it's .engine_url to substitute parameters within the original engine url.

    For example, this could be used to resolve a secret:

    def my_env_resolver(unresolved_engine_url):
        '''finds params starting like '{env_xxx}' and replaces it with the content of the
        environmental variable 'xxx'
        '''
        # .... code not shown

    >>> import ayeaye
    >>> from ayeaye.connectors.sqlalchemy_database import SqlAlchemyDatabaseConnector
    >>> ayeaye.connect_resolve.connector_resolver.add(my_env_resolver)
    >>> x = SqlAlchemyDatabaseConnector(engine_url="mysql://root:{env_secret_password}@localhost/my_database")
    >>> x.engine_url
    'mysql://root:p4ssw0rd@localhost/my_database'
    >>>

    Example using named attributes-
    >>> import ayeaye
    >>> from ayeaye.connectors.sqlalchemy_database import SqlAlchemyDatabaseConnector
    >>> ayeaye.connect_resolve.connector_resolver.add(my_env_resolver)

    """

    def __init__(self):
        self.unnamed_callables = []
        self._attr = {}

    def __getattr__(self, attr):
        if attr not in self._attr:
            raise AttributeError(f"Unknown attribute: '{attr}'")
        return self._attr[attr]

    def needs_resolution(self, engine_url):
        return engine_url is None or (isinstance(engine_url, str) and '{' in engine_url)

    def resolve_engine_url(self, unresolved_engine_url):
        """
        Fully resolve to an engine_url that contains no further parameters or raise an exception
        if not possible to resolve all.
        @return: (str) engine_url
        """
        engine_url = unresolved_engine_url
        for r_callable in self.unnamed_callables:
            engine_url = r_callable(engine_url)
            if not self.needs_resolution(engine_url):
                return engine_url

        # warning - don't put the partially resolved engine url into stack trace as it might
        # contain secretes.
        raise ValueError(f"Couldn't fully resolve engine URL. Unresolved: {unresolved_engine_url}")

    def add(self, *args, **kwargs):
        """
        @param *args: a callable that will have single argument (engine_url (str)) passed
            to it and must return (str) with anything that can be resolved having been resolved.
            There will be a chain of these resolvers that will be used in turn until there are no
            more parameters that need resolution.
        @param **kwargs: add named attributes. These could be instances, functions or plain variables.
            For an example, see docstring for class.
        """
        for resolver_callable in args:
            assert callable(resolver_callable)
            self.unnamed_callables.append(resolver_callable)

        for attribute_name, attribute_value in kwargs.items():
            if attribute_name in self._attr:
                raise ValueError(f"Attempted to set existing attribute: {attribute_name}")
            self._attr[attribute_name] = attribute_value

    def context(self, *args, **kwargs):
        """
        Use a resolver_callable just for the duration of a with statement. e.g.

        c = CsvConnector(engine_url="csv://my_path/data_{data_version}.csv")
        with connector_resolver.context(m_resolver):
            assert 'csv://my_path/data_1234.csv' == c.engine_url

        @see :method:`TestConnectors.test_resolve_engine_url` for an example.

        @see :method:`add` for args and kwargs
        """
        parent = self

        class ConnectorResolverContext:
            def __init__(self):
                self.args_count = None
                self.named_attr = None

            def __enter__(self):
                self.args_count = len(args)
                self.named_attr = kwargs
                parent.add(*args, **kwargs)

            def __exit__(self, exc_type, exc_val, exc_tb):

                for _ in range(self.args_count):
                    del parent.unnamed_callables[-1]

                for attr_name in self.named_attr.keys():
                    del parent._attr[attr_name]

        return ConnectorResolverContext()


# global provider of context
connector_resolver = ConnectorResolver()

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

    """
    def __init__(self):
        self.resolver_callables = []

    def needs_resolution(self, engine_url):
        return engine_url is None or (isinstance(engine_url, str) and '{' in engine_url)

    def resolve_engine_url(self, unresolved_engine_url):
        """
        Fully resolve to an engine_url that contains no further parameters or raise an exception
        if not possible to resolve all.
        @return: (str) engine_url
        """
        engine_url = unresolved_engine_url
        for r_callable in self.resolver_callables:
            engine_url = r_callable(engine_url)
            if not self.needs_resolution(engine_url):
                return engine_url

        # warning - don't put the partially resolved engine url into stack trace as it might
        # contain secretes.
        raise ValueError(f"Couldn't fully resolve engine URL. Unresolved: {unresolved_engine_url}")

    def add(self, resolver_callable):
        """
        @param resolver_callable: a caller that will have single argument (engine_url (str)) passed
            to it and must return (str) with anything that can be resolved having been resolved.
            There will be a chain of these resolvers that will be used in turn until there are no
            more parameters that need resolution.
        """
        assert callable(resolver_callable)
        self.resolver_callables.append(resolver_callable)
    
    def context(self, resolver_callable):
        """
        Use a resolver_callable just for the duration of a with statement. e.g.

        c = CsvConnector(engine_url="csv://my_path/data_{data_version}.csv")
        with connector_resolver.context(m_resolver):
            assert 'csv://my_path/data_1234.csv' == c.engine_url

        @see :method:`TestConnectors.test_resolve_engine_url` for an example.

        @param resolver_callable: @see :method:`add` for params and returns of callable.
        """
        parent = self
        class ConnectorResolverContext:
            def __enter__(self):
                parent.add(resolver_callable)
            def __exit__(self, exc_type, exc_val, exc_tb):
                del parent.resolver_callables[-1]

        return ConnectorResolverContext()

# global provider of context
connector_resolver = ConnectorResolver()

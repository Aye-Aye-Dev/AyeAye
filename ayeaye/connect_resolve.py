class ConnectorResolver:
    """
    Lazy substitution of parameters within engine_urls.

    Normal usage of this module is for a single global instance to be available to all instances of
    all the subclasses of :class:`DataConnector`. This is provided as-

    >>> from ayeaye.connect_resolve import connector_resolver

    This instance is given `resolver_callable`s at runtime and these are evaluated on demand by
    subclasses of DataConnector (i.e. connections to datasets) when they access the `engine_url`
    attribute.

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
    >>> ayeaye.connect_resolve.connector_resolver.add(env_secret_password="supersecret")
    >>> x = SqlAlchemyDatabaseConnector(engine_url="mysql://root:{env_secret_password}@localhost/my_database")
    >>> x.engine_url
    'mysql://root:supersecret@localhost/my_database'
    >>>

    """

    def __init__(self):
        self._clear_state()

    def _clear_state(self):
        self.unnamed_callables = []
        self._attr = {}

    def brutal_reset(self):
        """
        Discard any callables or attributes that have been set. This method is really for unit
        tests to stop a failed test effecting the 'clear' state for a subsequent test. Be careful
        not to call this other times.
        """
        self._clear_state()

    def __getattr__(self, attr):
        if attr not in self._attr:
            return DeferredResolution(self, attr)
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

        for k, v in self._attr.items():
            template_var = f"{{{k}}}"
            if template_var in engine_url:
                engine_url = engine_url.replace(template_var, v)
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
        @param **kwargs: add named attributes. The key is the templated variable e.g. "..{key}..."
            and the value is a variable to substitute into place.
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
            """
            Keep track of a temporary resolver.

            Can be used with a 'with statement' or by calling :method:`start` and :method:`finish`.
            @see :method:`TestResolve.test_without_with_statement` for an example.
            """

            def __init__(self):
                self.args_count = None
                self.named_attr = None

            def start(self):
                """
                Add a new resolver callable to the temporary context.
                """
                self.args_count = len(args)
                self.named_attr = kwargs
                parent.add(*args, **kwargs)

            def finish(self):
                """
                Clear this context away from the ConnectResolve instance.
                """
                # TODO check the right one is being cleared. This is currently making the assumption
                # that the last item is the right item.
                for _ in range(self.args_count):
                    del parent.unnamed_callables[-1]

                for attr_name in self.named_attr.keys():
                    del parent._attr[attr_name]

            def __enter__(self):
                self.start()

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.finish()

        return ConnectorResolverContext()


class DeferredResolution:
    """
    Avoid the catch 22 of :class:`ConnectorResolver`'s named attribute being needed by a class variable
    but having not been added to the connector_resolver instance.

    Save the calling until :method:`Connect._prepare_connection calls` :method:`evaluate`.
    """

    def __init__(self, calling_instance, requested_attrib):
        self.calling_instance = calling_instance
        self.requested_attrib = requested_attrib

        # for now this isn't a general pattern, just one level down is a method which is passed
        # args and kwargs or it's a plain variable.
        self.second_level_attrib_name = None
        self.method_args = None
        self.method_kwargs = None

    def __getattr__(self, attrib_name):
        self.second_level_attrib_name = attrib_name

        def callable_might_be_needed(*args, **kwargs):
            self.method_args = args
            self.method_kwargs = kwargs
            return self

        return callable_might_be_needed

    def __call__(self):
        original_attrib = getattr(self.calling_instance, self.requested_attrib)
        if self.method_args or self.method_kwargs:
            # attrib was a method
            target_method = getattr(original_attrib, self.second_level_attrib_name)
            r = target_method(*self.method_args, **self.method_kwargs)
            return r
        else:
            # just return the attrib
            return original_attrib


# global provider of context
connector_resolver = ConnectorResolver()

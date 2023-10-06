import copy
import json
import re
import warnings


class ConnectorResolver:
    """
    Lazy substitution of template parameters.

    Normal usage of this module is for a single global instance to be available to all instances of
    all the subclasses of :class:`DataConnector`. This is provided as-

    >>> from ayeaye.connect_resolve import connector_resolver

    This instance is given a mapping (i.e. key value pairs in a dictionary) or it is given a
    `resolver_callable` that will be called only when needed. This call is most likely to be by a
    subclasses of DataConnector (i.e. connections to datasets) when they access the `engine_url`
    attribute.

    Example using named attributes-

    >>> import ayeaye
    >>> from ayeaye.connectors.csv_connector import CsvConnector
    >>> ayeaye.connect_resolve.connector_resolver.add(build_id="20210616A")
    >>> x = CsvConnector(engine_url="csv:///data/build_{build_id}/product_codes.csv")
    >>> x.engine_url
    'csv:///data/build_20210616A/product_codes.csv'
    >>>

    Example using a callable to resolve a secret:

    def my_env_resolver(unresolved_engine_url):
        '''finds params starting like '{env_xxx}' and replaces it with the content of the
        environmental variable 'xxx'
        '''
        # .... code not shown

    >>> import ayeaye
    >>> from ayeaye.connectors.sqlalchemy_database import SqlAlchemyDatabaseConnector
    >>> ayeaye.connect_resolve.connector_resolver.add_secret(my_env_resolver)
    >>> x = SqlAlchemyDatabaseConnector(engine_url="mysql://root:{env_secret_password}@localhost/my_database")
    >>> x.engine_url
    'mysql://root:p4ssw0rd@localhost/my_database'
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
        if callable(engine_url):
            return True

        return engine_url is None or (isinstance(engine_url, str) and "{" in engine_url)

    def resolve_engine_url(self, unresolved_engine_url):
        """
        old name, use :meth:`resolve` instead
        """
        warnings.warn(
            "'resolve_engine_url' is deprecated use 'resolve' instead", DeprecationWarning
        )
        return self.resolve(unresolved_engine_url)

    def resolve(self, unresolved):
        """
        Fully resolve template variables to an string that contains no further parameters or raise
        an exception if not possible to resolve all.

        @param unresolved: (str) with templated variables. e.g. "csv://{my_variable}/abc.csv"

        @return: (str) engine_url
        """
        resolving = unresolved

        if not self.needs_resolution(resolving):
            # not a lot to do!
            return resolving

        for r_callable in self.unnamed_callables:
            resolving = r_callable(resolving)
            if not self.needs_resolution(resolving):
                return resolving

        for k, v in self._attr.items():
            template_var = f"{{{k}}}"
            if template_var in resolving:
                resolving = resolving.replace(template_var, v)
                if not self.needs_resolution(resolving):
                    return resolving

        # warning - don't put the partially resolved engine url into stack trace as it might
        # contain secretes.
        missing_vars = ",".join(re.findall("{.+?}", resolving))
        msg = (
            f"Couldn't fully resolve engine URL. Unresolved: {unresolved}. "
            f"Missing template variables are: {missing_vars}"
        )
        raise ValueError(msg)

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

            if not isinstance(attribute_name, (int, str)):
                raise ValueError(
                    f"templated variable '{attribute_name}' needs to be string or int."
                )

            self._attr[attribute_name] = attribute_value

    def add_secret(self, *args, **kwargs):
        """
        When an :class:`ayeaye.Model` is locked secrets shouldn't be included in the locking data.
        This method is analogous to :meth:`add` but keeps a reference to exclude these items
        from the :meth:`capture_context`.

        @see :meth:`add` for arguments.
        """
        raise NotImplementedError("TODO")

    def capture_context(self):
        """
        Return a JSON safe dictionary of context variables.

        The returned dictionary is expected to be serialised as part of :meth:`ayeaye.Model.lock`.

        @return (dict)
        """
        json_safe_types = (str, int, float, bool)

        # TODO: secrets - these shouldn't be returned

        # work in progress. Can only handle static mappings and not callables.
        if len(self.unnamed_callables) > 0:
            # alt approach is for ayeaye.Model to implement load_locking and unload_locking methods
            # and for the mapper dictionary to support callables and overlay of locking info
            raise NotImplementedError("Can't serialise callables - alternative approach needed.")

        for attribute_name, attribute_value in self._attr.items():
            # attrib names are checked in :meth:`add`
            if isinstance(attribute_value, json_safe_types):
                continue

            try:
                # sure fire way to check if it can be serialised into JSON
                _ = json.dumps(attribute_value)
            except TypeError:
                raise ValueError(f"Non-JSON serialisable data type found in '{attribute_name}'")

        # copy to make a snapshot as context manager will change _attr
        return {"mapper": copy.copy(self._attr)}

    def context(self, *args, **kwargs):
        """
        Use a resolver_callable just for the duration of a with statement. e.g.

        c = CsvConnector(engine_url="csv://my_path/data_{data_version}.csv")
        with connector_resolver.context(m_resolver):
            assert 'csv://my_path/data_1234.csv' == c.engine_url

        @see :meth:`TestConnectors.test_resolve_engine_url` for an example.

        @see :meth:`add` for args and kwargs

        Warning - not yet thread-safe as the global state is altered for the duration of the
        context manager. Can be fixed with thread local variables.
        """

        class _LocalResolverContext:
            """
            Keep track of a temporary resolver.

            Can be used with a 'with statement' or by calling :meth:`start` and :meth:`finish`.
            @see :meth:`TestResolve.test_without_with_statement` for an example.
            """

            def __init__(self, parent):
                self.context_args = []
                self.context_kwargs = {}
                self.args_count = None
                self.named_attr = None
                self.parent = parent

            def add(self, *a_args, **a_kwargs):
                """
                @see :meth:`ConnectorResolver.add`
                """
                if a_args:
                    self.context_args.extend(a_args)

                if a_kwargs:
                    self.context_kwargs.update(a_kwargs)

            def start(self):
                """
                Add a new resolver callable to the temporary context.
                """
                self.args_count = len(self.context_args)
                self.named_attr = self.context_kwargs
                self.parent.add(*self.context_args, **self.context_kwargs)

            def finish(self):
                """
                Clear this context away from the ConnectResolve instance.
                """
                # TODO check the right one is being cleared. This is currently making the assumption
                # that the last item is the right item.
                for _ in range(self.args_count):
                    del self.parent.unnamed_callables[-1]

                for attr_name in self.named_attr.keys():
                    del self.parent._attr[attr_name]

            def __enter__(self):
                self.start()

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.finish()

        isolated_local_context = _LocalResolverContext(parent=self)
        isolated_local_context.add(*args, **kwargs)
        return isolated_local_context


class DeferredResolution:
    """
    Avoid the catch 22 of :class:`ConnectorResolver`'s named attribute being needed by a class variable
    but having not been added to the connector_resolver instance.

    Save the calling until :meth:`Connect._prepare_connection calls` :meth:`evaluate`.
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

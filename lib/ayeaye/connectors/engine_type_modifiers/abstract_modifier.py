class AbstractEngineTypeModifier:
    """
    engine_urls can be prefixed with modifier that slightly changes how the underlying
    DataConnector works.

    For example-
        engine_url="gz+ndjson:///data/myfile.ndjson.gz"

    Would transparently compress/decompress on the fly but is otherwise using the
    :class:`ayeaye.Connectors.NdjsonConnector` connector.

    Modifier classes are subclasses of this `AbstractEngineTypeModifier` class and their :meth:`apply` is
    applied to the target `DataConnector` in the same way that python decorators work behind the
    scenes-

    e.g.

    >>> my_connector = NdjsonConnector(**some_kwargs)
    >>> a_modifer = ExampleCloudStorageModifier()
    >>> my_connector = a_modifer.apply(connector=my_connector)
    """

    @staticmethod
    def provides_support(connector_cls, modifier_labels):
        """
        Does this (i.e. `self`) engine type modifier support all of the `modifier_labels` for the
        `connector_cls`?

        @param connector_cls: (subclass of :class:`DataConnector`)
            The target data connector for the modifier.

        @param modifier_labels (list of str):
            Named properties that must all be supported by the returned modifier. It's possible
            that the order of these matters, hence a list.
            e.g. ['s3', 'gz']

        @return bool
        """
        raise NotImplementedError("Must be implemented by subclasses")

    @classmethod
    def apply(cls, connector_cls, modifier_labels):
        """
        Build a `DataConnector` like object that can take the place of a true :class:`DataConnector`
        instance.

        @param connector_cls: (subclass of :class:`DataConnector`)
            The target data connector for the modifier.

        @param modifier_labels (list of str):
            Named properties that must all be supported by the returned modifier. It's possible
            that the order of these matters, hence a list.
            e.g. ['s3', 'gz']


        @return: a :class:`DataConnector` like object.
        """

        # create engine_type for dynamic class
        mod_labels = "+".join(modifier_labels)
        supported_engines = (
            connector_cls.engine_type
            if isinstance(connector_cls.engine_type, list)
            else [connector_cls.engine_type]
        )
        dyn_engine_type = [mod_labels + "+" + engine for engine in supported_engines]

        class DynamicConnector(cls, connector_cls):
            requested_modifier_labels = modifier_labels
            engine_type = dyn_engine_type

            def __init__(self, *args, **kwargs):
                cls.__init__(self)
                connector_cls.__init__(self, *args, **kwargs)

        return DynamicConnector

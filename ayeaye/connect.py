import copy
from enum import Enum

from ayeaye.connectors import connector_factory
from ayeaye.connectors.models_connector import ModelsConnector
from ayeaye.connectors.multi_connector import MultiConnector
from ayeaye.connectors.placeholder import PlaceholderDataConnector


class Connect:
    """
    Connect to a dataset or modelset.

    A dataset is a grouping of related data. What makes data 'related' will be down to the
    subject-domain. Datasets have a type - it could be a JSON document, a CSV file or a database.

    A modelset is one or more :class:`Model`(s).

    The main responsibility of :class:`Connect` is to provide a concrete subclass of
    :class:`DataConnector` or :class:`ModelConnector`. :class:`DataConnector` in turn provides
    access to operations on that dataset type. e.g. read, write etc.

    :class:`Connect` can be used standalone (see below) but is really designed to be used as a
    class variable in an :class:`ayeaye.Model`:

    ```
        class FavouriteColours(ayeaye.Model):
            favourite_colours = ayeaye.Connect(engine_url='csv://data/favourite_colours.csv')
    ```

    An instance of :class:`Connect` as a class variable in an :class:`ayeaye.Model` is a
    declaration of a model's use of a dataset. In Python, class variables shouldn't be used as
    'dynamic' instance variables so :class:`Connect` is a descriptor
    (https://docs.python.org/3/howto/descriptor.html) and the instantiation of the underlying
    connection (concrete subclass of :class:`DataConnector`) is evaluated on demand.

    It is also possible to use :class:`Connect` in *standalone* mode. This is as a convenience, in
    particular for evaluating datasets, for example in a Jupyter notepad:
    ```
    for row in ayeaye.Connect(engine_url='csv://data/favourite_colours.csv'):
      print(row.colour)
    ```

    For secrets management @see :class:`ConnectorResolver`.
    """
    mutually_exclusive_selectors = ['ref', 'engine_url', 'models']

    class ConnectBind(Enum):
        MODEL = "MODEL"
        STANDALONE = "STANDALONE"
        NEW = "NEW"

    def __init__(self, **kwargs):
        """
        typical kwargs are 'ref', 'engine_url', 'access' TODO
        """
        self.base_constructor_kwargs = copy.copy(kwargs)
        self._construct(**kwargs)

    def _construct(self, **kwargs):
        """
        setup -- can be called either on true construction or when variables are overlaid on a
        (possibly cloned) instance of :class:`Connect`.
        """
        # :class:`Connect` is responsible for resolving 'ref' into an engine_url via a
        # data catalogue. So 'ref' isn't passed to data type specific connectors (i.e.
        # subclasses of :class:`DataConnector`)

        # these are passed to the data type specific connectors
        self.relayed_kwargs = {**self.base_constructor_kwargs, **kwargs}

        # check construction args are valid

        # mutually exclusive args
        a = [self.relayed_kwargs.get(s) is not None for s in self.mutually_exclusive_selectors]
        mandatory_args_count = sum(a)
        if mandatory_args_count > 1:
            raise ValueError('The kwargs ref, engine_url and models are mutually exclusive.')

        self.ref = self.relayed_kwargs.pop('ref', None)
        self._standalone_connection = None  # see :method:`data`
        self._parent_model = None

    def __repr__(self):
        args = ', '.join([f"{k}={v}" for k, v in self.base_constructor_kwargs.items()])
        return f'<Connect({args})>'

    def update(self, **kwargs):
        """
        Overlay (and overwrite) kwarg on existing instance.
        @returns None
        """
        # reminder to me: I've gone in circles here on returning a copy (factory style) and it's
        # not a good idea
        self._construct(**kwargs)

    def clone(self, **kwargs):
        """
        Overlay (and overwrite) kwarg onto a copy of the existing instance.

        This is typically used when :class:`Connect` objects are being used as class variables to
        refer to the same dataset multiple times in a single model.

        @see :method:`TestModels.test_double_usage`

        @return (instance of :class:`Connect`)
        """
        new_instance = copy.copy(self)
        new_instance._construct(**{**self.relayed_kwargs, **kwargs})
        return new_instance

    def connect_id(self):
        """
        Create an identity reference which is used when examining if separate Connect instances
        are actually referring to the same dataset/models.

        @return: (str)
        """
        for s in self.mutually_exclusive_selectors:
            if s in self.relayed_kwargs:
                # note, self.relayed_kwargs[s] could be a callable. Whatever it it needs to
                # deterministically cast to a string.
                return f"{s}:{self.relayed_kwargs[s]}"
        return "empty:"

    def __hash__(self):
        return hash(self.connect_id())

    def __eq__(self, other):
        if type(self) is type(other):
            return self.connect_id() == other.connect_id()
        return False

    def __copy__(self):
        c = self.__class__(**self.base_constructor_kwargs)
        c.relayed_kwargs = copy.copy(self.relayed_kwargs)
        return c

    def __get__(self, instance, instance_class):
        if instance is None:
            # class method called.
            # This means `self` is currently an attribute of the class (so NOT an instance
            # variable).
            #
            # Class variables are kind of like constants because they are shared between multiple
            # instances of the class and you shouldn't mutate them so just return self.
            return self

        if self.connection_bind == Connect.ConnectBind.STANDALONE:
            raise ValueError("Attempt to connect as a model when already initiated as standalone")

        ident = id(self)
        if ident not in instance._connections:

            instance._connections[ident] = self._prepare_connection()

            # a Connect belongs to zero or one ayeaye.Model. Link in both directions.
            instance._connections[ident]._parent_model = instance
            self._parent_model = instance

        return instance._connections[ident]

    def __set__(self, instance, new_connection):
        """
        Replace an instance of :class:`ayeaye.Model`'s :class:`ayeaye.Connect` with another
        instance of `Connect`.
        """
        if not isinstance(new_connection, self.__class__):
            my_class = self.__class__.__name__
            raise ValueError(f'Only {my_class} instances can be set')

        self.__init__(**new_connection.relayed_kwargs)
        ident = id(self)
        instance._connections[ident] = self._prepare_connection()

    def _prepare_connection(self):
        """
        Resolve everything apart from secrets needed to access the engine behind this dataset.

        @return: (instance subclass of :class:`DataConnector`) or (None) when resolve not yet
            possible.
        """
        if self.ref is not None:
            raise NotImplementedError(("Sorry! Dataset discovery (looking up engine_url from ref) "
                                       "hasn't been written yet."
                                       ))
        detached_kwargs = None

        if 'models' in self.relayed_kwargs:
            connector_cls = ModelsConnector

        elif 'engine_url' not in self.relayed_kwargs or self.relayed_kwargs['engine_url'] is None:
            connector_cls = PlaceholderDataConnector

        else:

            if callable(self.relayed_kwargs['engine_url']):
                engine_url = self.relayed_kwargs['engine_url']()

                # make the resolved engine_url available to the final connector.
                #
                # The callable isn't expected to have __deep_copy__ method so a selective copy
                # happens after the callable has built the engine_url.
                # The callable is left in place because it's a class variable so could be resolved
                # again under a different context) and the engine_url used to build the Connector
                # instance.
                detached_kwargs = {}
                for k, v in self.relayed_kwargs.items():
                    if k == 'engine_url':
                        detached_kwargs[k] = engine_url
                    else:
                        detached_kwargs[k] = copy.deepcopy(v)

            else:
                engine_url = self.relayed_kwargs['engine_url']

            if isinstance(engine_url, list):
                # compile time list of engine_url strings
                # might be callable or a dict or set in the future
                connector_cls = MultiConnector
            else:
                connector_cls = connector_factory(engine_url)

        if not detached_kwargs:
            detached_kwargs = copy.deepcopy(self.relayed_kwargs)

        connector = connector_cls(**detached_kwargs)
        connector._connect_instance = self
        return connector

    @property
    def connection_bind(self):
        """
        Raises a ValueError if an indeterminate state is found.

        @return: (ConnectBind)

        @see class's docstring.

        An instance of :class:`Connect` can be a class variable for an :class:`ayeaye.Model`
        or
        Standalone mode
        or
        Not yet determined
        """

        if self._parent_model is None and self._standalone_connection is None:
            return Connect.ConnectBind.NEW

        if self._standalone_connection is not None:
            return Connect.ConnectBind.STANDALONE

        if self._parent_model is not None:
            return Connect.ConnectBind.MODEL

        msg = ('Parent already attached and standalone connection is present. This'
               ' shouldn\'t ever happen. Please let us know how it did!'
               )
        raise ValueError(msg)

    def connect_standalone(self):
        """
        Make a standalone Connect.

        Connect is normally used as part of an class:`ayeaye.Model` and will connect to the target
        dataset on demand. It's also possible to use Connect as a standalone instance which
        proxies to the target dataset's instance. The standalone version is also stood up on demand
        but it can be done explicitly with this method if Connect is short cutting the as-a-proxy
        incorrectly. See :method:`__getattr__`.
        """
        if self.connection_bind == Connect.ConnectBind.MODEL:
            raise ValueError("Attempt to connect as standalone when already bound to a model")

        if self.connection_bind == Connect.ConnectBind.NEW:
            self._standalone_connection = self._prepare_connection()

    def __getattr__(self, attr):
        """
        proxy through to subclass of :class:`DataConnector` when used as a standalone Connect
        (i.e. not a class variable on :class:`ayeaye.Model`).
        """
        if self.connection_bind == Connect.ConnectBind.MODEL:
            cls_name = self.__class__.__name__
            attrib_error_msg = f"'{cls_name}' object has no attribute '{attr}'"
            raise AttributeError(attrib_error_msg)

        # Short cut to proxy
        # avoid instantiating the target DataConnector if the attribute that is being accessed
        # is known to Connect.
        # TODO - maybe make a list of permitted non-proxy attribs because the target DataConnector
        # might do things to certain attribs on construction so the real version would differ.
        # e.g. ConnectorResolver with engine_urls
        if self.connection_bind == Connect.ConnectBind.NEW and attr in self.relayed_kwargs:
            return self.relayed_kwargs[attr]

        self.connect_standalone()
        return getattr(self._standalone_connection, attr)

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        """
        more intuitive use of the data behind this Connect. i.e. proxy to a DataConnector.
        e.g.
        ...
        for record in Connect(ref="my_dataset"):
            print(record)
        """
        self.connect_standalone()
        yield from self._standalone_connection

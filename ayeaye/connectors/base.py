from abc import ABC, abstractmethod
from enum import Enum

from ayeaye.connect_resolve import connector_resolver


class AccessMode(Enum):
    READ = 'r'
    WRITE = 'w'
    READWRITE = 'rw'


class DataConnector(ABC):
    engine_type = None  # must be defined by subclasses
    optional_args = {}  # subclasses should specify their optional kwargs. Values in this dict are
    # default values.

    def __init__(self, engine_url=None, access=AccessMode.READ, **kwargs):
        """
        API to interact with AyeAye-compatible data sources
        :param engine_url (string): The file or URI to the location of the dataset
        :param access (AccessMode): Whether the dataset accessed through this Connector is for
                input or output
        **kwargs are any params needed by subclasses

        Note that subclasses must call this constructor and should 'pop' their arguments so that
        none are left unprocessed. Any which are specified in optional_args will be set as
        attributes by this super constructor.
        """
        self.access = access
        # engine_urls may need local resolution to find a particular version or to replace
        # placeholders with secrets
        self._unresolved_engine_url = engine_url
        self._fully_resolved_engine_url = None

        self._connect_instance = None  # set when :class:`ayeaye.Connect` builds subclass instances

        if isinstance(self._unresolved_engine_url, str):
            engine_type = [self.engine_type] if isinstance(self.engine_type, str) \
                else self.engine_type
            if not any([self._unresolved_engine_url.startswith(et) for et in engine_type]):
                raise ValueError("Engine type mismatch")

        # process optional arguments with their defaults
        for arg in self.__class__.optional_args:
            setattr(self, arg, kwargs.pop(arg, self.__class__.optional_args[arg]))

        # Subclasses should consume any kwargs before this constructor is invoked
        if len(kwargs) > 0:
            raise ValueError('Unexpected arguments')

    def __del__(self):
        self.close_connection()

    @property
    def engine_url(self):
        """
        A fully resolved engine url contains everything needed to connect to the data source. This
        includes secrets like username and password.

        Don't store the result of this property when locking or similar. @see :method:`engine_url_public` 

        @return: (str) the fully resolved engine url.
        """
        if self._fully_resolved_engine_url is None:
            if connector_resolver.needs_resolution(self._unresolved_engine_url):
                # local resolution is still needed
                resolved = connector_resolver.resolve_engine_url(self._unresolved_engine_url)
                self._fully_resolved_engine_url = resolved
            else:
                self._fully_resolved_engine_url = self._unresolved_engine_url

        return self._fully_resolved_engine_url

    @property
    def engine_url_public(self):
        raise NotImplementedError("TODO")

    def __call__(self, **kwargs):
        """
        Rebuild this (subclass of) DataConnector. It can only be used if self was built by
        :class:`ayeaye.Connect` because Connect determines the subclass based on engine type.
        If the call used the same engine_type as `self` then this wouldn't be needed.
        """
        if self._connect_instance is None:
            raise ValueError("Connect instance not referenced. See :method:`__call__ for more")

        self._connect_instance._construct(**kwargs)
        if self._connect_instance._parent_model is not None:
            parent_model = self._connect_instance._parent_model
            ident = id(self._connect_instance)
            # remove old Connector, it will be re-built on access
            del parent_model._connections[ident]

    @abstractmethod
    def connect(self):
        """
        Open resource handles used to access the dataset. e.g. network or filesystem connection.
        These resources are help open by the subclass.
        """
        pass

    def close_connection(self):
        """
        Explicitly close the connection to the dataset rather than just wait for the process to end.
        """
        pass

    @abstractmethod
    def __len__(self):
        raise NotImplementedError("TODO")

    @abstractmethod
    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        """
        more intuative use of the data behind each the DataConnector.
        e.g.
        ...
        for record in self.my_dataset:
            print(record)
        """
        self.connect()
        return self.data

    @property
    @abstractmethod
    def data(self):
        """
        Return the entire dataset. The returned objects could be lazy evaluated. Use of this method
        will get ugly with large datasets. Better to use the :method:`__iter__` iterator.

        @return: mixed
        """
        raise NotImplementedError("TODO")

    def as_pandas(self):
        """
        Similar to :method:`data` but as a Pandas dataframe.

        @return: (Pandas dataframe)
        """
        raise NotImplementedError("Not available for all datasets or might need to be written")

    @property
    @abstractmethod
    def schema(self):
        """Return the schema of whatever data source we're interacting with"""
        raise NotImplementedError("TODO")

    @property
    def progress(self):
        """
        Return a number between and including 0 to 1 as simplified representation of how much of
        the dataset has been read.

        Optional property that can be implemented by subclasses. When available it returns float
        from 0 -> 1 describing the position within the dataset. The value is only meaningful when
        the dataset is in READ mode and records are being iterated through. The value isn't
        expected to be really accurate as some reads will cache data so exact file positions wont
        be available.

        :returns: (float) or None when not available.
        """
        return None

    @property
    def connect_instance(self):
        """
        Instances of subclasses of :class:`DataConnector` are usually built by
        :class:`ayeaye.Connect`. `connect_instance` is a reference to make it easy to tweak an
        existing connect on a model. See :method:`TestConnect.test_update_by_replacement` for an
        example.
        """
        return self._connect_instance

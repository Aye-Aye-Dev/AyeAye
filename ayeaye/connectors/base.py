from abc import ABC, abstractmethod
from enum import Enum


class AccessMode(Enum):
    READ = 'r'
    WRITE = 'w'
    READWRITE = 'rw'


class DataConnector(ABC):
    engine_type = None  # must be defined by subclasses
    optional_args = {}  # subclasses should specify their optional kwargs. Values in this dict are default values.

    def __init__(self, engine_url=None, access=AccessMode.READ, **kwargs):
        """
        API to interact with AyeAye-compatible data sources
        :param engine_url (string): The file or URI to the location of the dataset
        :param access (AccessMode): Whether the dataset accessed through this Connector is for
                input or output
        **kwargs are any params needed by subclasses

        Note that subclasses must call this constructor and should 'pop' their arguments so that none are left
        unprocessed. Any which are specified in optional_args will be set as attributes by this super constructor.
        """
        self.access = access
        self.engine_url = engine_url

        engine_type = [self.engine_type] if isinstance(self.engine_type, str) else self.engine_type
        if not any([self.engine_url.startswith(et) for et in engine_type]):
            raise ValueError("Engine type mismatch")

        # process optional arguments with their defaults
        for arg in self.__class__.optional_args:
            setattr(self, arg, kwargs.pop(arg, self.__class__.optional_args[arg]))

        # Subclasses should consume any kwargs before this constructor is invoked
        if len(kwargs) > 0:
            raise ValueError('Unexpected arguments')

    @abstractmethod
    def connect(self):
        """Open network or filesystem or other connection to datasource. The connection is expected
        to be cached by subclasses"""
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

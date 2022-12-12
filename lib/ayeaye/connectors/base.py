from enum import Enum
import glob
import types

from ayeaye.ignition import Ignition, EngineUrlCase, EngineUrlStatus


class AccessMode(Enum):
    READ = "r"
    WRITE = "w"
    READWRITE = "rw"


class AbstractExpandEnginePattern:
    """
    Mixin to support wildcard engine_urls.
    https://www.pythontutorial.net/python-oop/python-mixin/

    When an engine_url contains pattern matching characters the :class:`MultiConnector` should
    be used after the :class:`ConnectorResolver` singleton has resolved context variables.

    Subclasses of this abstract class are given responsibility to pattern match as filesystems
    and blob services use different methodologies to do this.
    """

    def has_multi_engine_pattern(self):
        """
        @return: bool
            the engine_url for the class implementing this mixin has a pattern
        """
        raise NotImplementedError("Must be implemented by subclasses")

    def expand_pattern(self):
        """
        @return: list of str
            engine_urls, each one becomes a connector (subclass of :class:`DataConnector`) in a
            :class:`MultiConnector`
        """
        raise NotImplementedError("Must be implemented by subclasses")


class FilesystemEnginePatternMixin(AbstractExpandEnginePattern):
    """
    Pattern match files and directories. @see :class:`AbstractExpandEnginePattern`
    """

    # List of characters that when found in an engine_url indicate it's a pattern matching url that
    # will result in multiple engine_urls. i.e. file:///data/*.csv
    # A :class:`MultiConnector` should be used.
    # Optionally defined by subclasses.
    pattern_characters = ["*", "?"]

    def has_multi_engine_pattern(self):
        for pattern_indicating_character in self.pattern_characters:
            if pattern_indicating_character in self.engine_url:
                return True
        return False

    def expand_pattern(self):

        # strip engine type
        # for now, they all need to have the same engine_type. Maybe engine_url starts
        # with `://` for auto detect based on file name.
        engine_type, engine_path_pattern = self.engine_url.split("://", 1)

        engine_url = []
        for engine_file in glob.glob(engine_path_pattern):
            engine_url.append(f"{engine_type}://{engine_file}")

        return engine_url


class DataConnector:
    engine_type = None  # must be defined by subclasses

    # subclasses should specify their optional kwargs. Values in this dict are default values.
    optional_args = {}

    # list/set of arg names that are present in optional_args and when they are assigned with a
    # callable this shouldn't be called by :class:`ayeaye.Connect` but should instead be passed
    # as is to the target subclass of :class:`ayeaye.DataConnector`.
    preserve_callables = []

    # TODO - make it possible for internal variable name to not match kwarg name, e.g. schema -> self._schema
    # TODO - these aren't always optional, handling missing mandatory args here

    def __init__(self, engine_url=None, access=AccessMode.READ, method_overlay=None, **kwargs):
        """
        API to interact with AyeAye-compatible data sources
        :param engine_url (string): The file or URI to the location of the dataset
        :param access (AccessMode): Whether the dataset accessed through this Connector is for
                input or output
        :param method_overlay (callable or list of callables): Callables are patched onto `self` as methods.
                e.g.
                    my_dataset = ayeaye.Connect(..., method_overlay=[accessor_method])
                    ...
                    my_dataset.accessor_method() now exists

        **kwargs are any params needed by subclasses

        Note that subclasses must call this constructor and should 'pop' their arguments so that
        none are left unprocessed. Any which are specified in optional_args will be set as
        attributes by this super constructor.
        """
        # set when :class:`ayeaye.Connect` builds subclass instances
        self._connect_instance = None  # instance of :class:`ayeaye.Connect`
        self._parent_model = None  # instance of :class:`ayeaye.Model`

        self.access = access

        # dynamically add methods to `self`
        if method_overlay:
            overlays = [method_overlay] if callable(method_overlay) else method_overlay
            for m_overlay in overlays:
                method_name = m_overlay.__name__
                setattr(self, method_name, types.MethodType(m_overlay, self))

        # engine_urls may need resolution of templated variables (typically secrets and paths). The
        # :class:`Ignition` module does this and makes URLs croxx-operating system compatible.
        self.ignition = Ignition(engine_url)

        if isinstance(engine_url, str):
            engine_type = (
                [self.engine_type] if isinstance(self.engine_type, str) else self.engine_type
            )
            if not any([engine_url.startswith(et) for et in engine_type]):
                raise ValueError("Engine type mismatch")

        # process optional arguments with their defaults
        for arg in self.__class__.optional_args:
            setattr(self, arg, kwargs.pop(arg, self.__class__.optional_args[arg]))

        # Subclasses should consume any kwargs before this constructor is invoked
        if len(kwargs) > 0:
            what_are_these = ", ".join(kwargs.keys())
            raise ValueError(f"Unexpected argument(s): '{what_are_these}'")

    def __del__(self):
        self.close_connection()

    @property
    def connect_instance(self):
        """
        Instances of subclasses of :class:`DataConnector` and :class:`ModelConnector` are usually
        built by :class:`ayeaye.Connect`. `connect_instance` is a reference to make it easy to
        tweak an existing connect on a model. See :method:`TestConnect.test_update_by_replacement`
        for an example.
        """
        return self._connect_instance

    @property
    def engine_url(self):
        """
        A fully resolved engine url contains everything needed to connect to the data source. This
        includes secrets like username and password.

        Don't store the result of this property when locking or similar. @see :class:`Ignition`.

        @return: (str) the fully resolved engine url.
        """
        status, e_url = self.ignition.engine_url_at_state(EngineUrlCase.FULLY_RESOLVED)
        if status == EngineUrlStatus.OK:
            return e_url
        elif status == EngineUrlStatus.NOT_AVAILABLE:
            raise ValueError("Engine URL not available")
        else:
            raise ValueError(f"Engine URL failed to resolve: {status}")

    def update(self, **kwargs):
        """
        Rebuild this (subclass of) DataConnector. It can only be used if self was built by
        :class:`ayeaye.Connect` because Connect determines the subclass based on the engine type.
        If the call used the same engine_type as `self` then this wouldn't be needed.

        The instance of :class:`Connect` that `self` is referencing is updated with `kwargs`. The
        current prepared DataConnect subclass (i.e. self) that was assigned to the parent model is
        thrown away because the new args probably alter how the dataset is connected to. It will
        re-initialise on demand.

        @param kwargs: anything you'd pass to :class:`Connect`
        @return: None
        """
        if self._connect_instance is None:
            raise ValueError("Connect instance not referenced. See :method:`update` for more")

        self._connect_instance.update(**kwargs)
        ident = id(self._connect_instance)
        if self._parent_model is not None:
            # remove old Connector, it will be re-built on access
            del self._parent_model._connections[ident]

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

    def __len__(self):
        raise NotImplementedError("TODO")

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
    def datasource_exists(self):
        msg = (
            "Can optionally be implemented by subclasses. "
            "Contribute if you need it! There is an example in JsonConnector."
        )
        raise NotImplementedError(msg)

"""
Created on 12 Feb 2020

@author: si
"""
from ayeaye.connectors.base import AccessMode, DataConnector
from ayeaye.connectors import connector_factory


class MultiConnector(DataConnector):
    engine_type = None  # See docstring in constructor
    preserve_callables = ["child_method_overlay"]

    def __init__(self, *args, **kwargs):
        """
        Connector to multiple other Connectors.

        For args: @see :class:`connectors.base.DataConnector`

        additional args for MultiConnector
         child_method_overlay - add a method to child connectors

        Connection information-
            Not normally initiated with `engine_type://...` url but by giving a list
            of engine_urls which each connect to another subclass of DataConnector
        """
        # MultiConnector passes a copy of the kwargs to each child connector. It's not known
        # until after Connect which ones are permitted by the target DataConnector so the
        # exception wont be raised until usage.
        base_kwargs = {
            "engine_url": kwargs.pop("engine_url", None),
            "access": kwargs.pop("access", AccessMode.READ),
            "method_overlay": kwargs.pop("method_overlay", None),
        }
        super().__init__(*args, **base_kwargs)

        self.connector_kwargs = kwargs

        # rename field to make allocation of overlay method explicit to either the parent
        # multi-connector or children
        method_overlay = self.connector_kwargs.pop("child_method_overlay", None)
        self.connector_kwargs["method_overlay"] = method_overlay

        self._child_data_connectors = None  # on connect
        self._child_dc_mapping = {}

    def connect(self):
        super().connect()
        if len(self.engine_url) != len(set(self.engine_url)):
            msg = "engine_url contains duplicates. Not yet supported."
            raise NotImplementedError(msg)

        if self._child_data_connectors is None:
            if not isinstance(self.engine_url, list):
                raise ValueError("Expected a list of engine_urls in self.engine_url")

            self._child_data_connectors = []
            for idx, engine_url in enumerate(self.engine_url):
                connector_cls = connector_factory(engine_url)
                connector = connector_cls(
                    engine_url=engine_url, access=self.access, **self.connector_kwargs
                )
                # all child connectors use the parent instance (i.e. the original ayeaye.Connect)
                connector._connect_instance = self._connect_instance

                self._child_data_connectors.append(connector)
                # this is the unresolved engine_url
                self._child_dc_mapping[engine_url] = idx

        else:
            # see if self._child_data_connectors is stale. The happens if engine_urls has been
            # changed since the last connect(). It's easier to detect the change on read than it is
            # to override the behaviour of engine_urls.append(..) to connect on demand.
            # Also, can't just drop and rebuild as they could be in use
            already_seen = len(self._child_dc_mapping)
            for idx, engine_url in enumerate(self.engine_url):
                if engine_url not in self._child_dc_mapping:
                    if idx < already_seen:
                        msg = f"Can't remap after engine_url removed for: {engine_url}"
                        raise NotImplementedError(msg)
                    else:
                        # new engine_url
                        connector_cls = connector_factory(engine_url)
                        connector = connector_cls(engine_url=engine_url, access=self.access)
                        # all child connectors use the parent instance (i.e. the original ayeaye.Connect)
                        connector._connect_instance = self._connect_instance
                        self._child_data_connectors.append(connector)
                        self._child_dc_mapping[engine_url] = idx

                elif self._child_dc_mapping[engine_url] != idx:
                    raise Exception("Please tell the AyeAye developers how this exception happens!")

    def close_connection(self):
        super().close_connection()
        if self._child_data_connectors:
            for c in self._child_data_connectors:
                c.close_connection()

        self._child_data_connectors = None  # on connect
        self._child_dc_mapping = {}

    def add_engine_url(self, engine_url):
        """
        Add a child connector at run time and return the resolved connector (to the new engine_url).

        A connector is associated with each `engine_url` passed to this method so duplicate
        `engine_url`s will all resolve to the same connector.

        multi_connector.engine_url.append(...) could also be used, it doesn't de-dupe `engine_url`s.

        Note that a previously used connector will retain it's connection state from it's previous
        usage.
        e.g. a child connector could have iterated part way through an input file.
        The connector could always be reset by closing with :method:`close_connection` and then
        opened again.

        @param engine_url: (str) unresolved engine_url (i.e. could contain {params} to be resolved
                by :class:`ayeaye.connect_resolve.ConnectorResolver`

        @return: (subclass of :class:`DataConnector`)
        """

        # Note - engine_url might not be fully resolved and multiple unresolved could resolve into
        # same url

        if engine_url in self._child_dc_mapping:
            idx = self._child_dc_mapping[engine_url]
            connector = self._child_data_connectors[idx]
            return connector

        self.engine_url.append(engine_url)
        self.connect()

        idx = self._child_dc_mapping[engine_url]
        connector = self._child_data_connectors[idx]
        return connector

    def __len__(self):
        """
        How many datasets is this MultiConnector holding
        """
        self.connect()
        return len(self._child_dc_mapping)

    def __getitem__(self, key):
        """
        @param key: (str) engine_url

        @return: (subclass of :class:`DataConnector`)
        """
        self.connect()
        idx = self._child_dc_mapping[key]
        connector = self._child_data_connectors[idx]
        return connector

    def __iter__(self):
        self.connect()
        for dc in self._child_data_connectors:
            yield dc

    @property
    def data(self):
        """
        @return: list of subclasses of :class:`DataConnector` objects.
        """
        self.connect()
        return self._child_data_connectors

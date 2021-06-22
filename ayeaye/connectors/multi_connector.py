'''
Created on 12 Feb 2020

@author: si
'''
from ayeaye.connectors.base import AccessMode, DataConnector
from ayeaye.connectors import connector_factory


class MultiConnector(DataConnector):
    engine_type = None  # See docstring in constructor

    def __init__(self, *args, **kwargs):
        """
        Connector to multiple other Connectors.

        For args: @see :class:`connectors.base.DataConnector`

        additional args for CsvConnector
         None

        Connection information-
            Not normally initiated with `engine_type://...` url but by giving a list
            of engine_urls which each connect to another subclass of DataConnector
        """
        # MultiConnector passes a copy of the kwargs to each child connector. It's not known
        # until after Connect which ones are permitted by the target DataConnector so the
        # exception wont be raised until usage.
        base_kwargs = {'engine_url': kwargs.pop('engine_url', None),
                       'access': kwargs.pop('access', AccessMode.READ)
                       }
        super().__init__(*args, **base_kwargs)

        self.connector_kwargs = kwargs
        self._child_data_connectors = None  # on connect
        self._child_dc_mapping = {}

    def connect(self):

        if len(self.engine_url) != len(set(self.engine_url)):
            msg = "engine_url contains duplicates. Not yet supported."
            raise NotImplementedError(msg)

        if self._child_data_connectors is None:
            if not isinstance(self.engine_url, list):
                raise ValueError("Expected a list of engine_urls in self.engine_url")

            self._child_data_connectors = []
            for idx, engine_url in enumerate(self.engine_url):
                connector_cls = connector_factory(engine_url)
                connector = connector_cls(engine_url=engine_url,
                                          access=self.access,
                                          **self.connector_kwargs)
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
                        self._child_data_connectors.append(connector)
                        self._child_dc_mapping[engine_url] = idx

                elif self._child_dc_mapping[engine_url] != idx:
                    raise Exception("Please tell the AyeAye developers how this exception happens!")

    def close_connection(self):

        if self._child_data_connectors:
            for c in self._child_data_connectors:
                c.close_connection()

        self._child_data_connectors = None  # on connect
        self._child_dc_mapping = {}

    def add_engine_url(self, engine_url):
        """
        A convenience method for adding engine_urls at run time and returning the resolved
        connector (to the new engine_url) in one call.

        multi_connector.engine_url.append(...) could also be used.

        @param engine_url: (str) unresolved engine_url (i.e. could contain {params} to be resolved
                by :class:`ayeaye.connect_resolve.ConnectorResolver`

        @return: (subclass of :class:`DataConnector`)
        """
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
        self.connect()
        return self._child_data_connectors

    @property
    def schema(self):
        raise NotImplementedError("TODO")

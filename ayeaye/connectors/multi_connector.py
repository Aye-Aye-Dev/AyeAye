'''
Created on 12 Feb 2020

@author: si
'''
from ayeaye.connectors.base import DataConnector, AccessMode
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
        super().__init__(*args, **kwargs)

        self.child_data_connectors = None # on connect

        if self.access != AccessMode.READ:
            raise NotImplementedError('Write access not yet implemented')

    def connect(self):

        if self.child_data_connectors is None:        
            if not isinstance(self.engine_url, list):
                raise ValueError("Expected a list of engine_urls in self.engine_url")
    
            self.child_data_connectors = []
            for engine_url in self.engine_url:
                connector_cls = connector_factory(engine_url)
                connector = connector_cls(engine_url=engine_url, access=self.access)
                self.child_data_connectors.append(connector)

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        self.connect()
        for dc in self.child_data_connectors:
            yield dc

    @property
    def data(self):
        self.connect()
        return self.child_data_connectors

    @property
    def schema(self):
        raise NotImplementedError("TODO")

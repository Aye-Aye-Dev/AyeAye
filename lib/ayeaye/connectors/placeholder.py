from ayeaye.connectors.base import DataConnector


class PlaceholderDataConnector(DataConnector):
    """
    For use when the engine_type isn't available yet. The DataConnector abstract class is a
    callable that can convert a DataConnector belonging to an :class`ayeaye.Model` into
    another subclass of DataConnector.
    """
    engine_type = None

    def connect(self):
        pass

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    @property
    def schema(self):
        return None

    @property
    def data(self):
        return None

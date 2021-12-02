from ayeaye.connectors.base import DataConnector


class FakeDataConnector(DataConnector):
    """
    Fake connector used in unit tests.
    """

    engine_type = "fake://"
    optional_args = {
        "quantum_accelerator_module": None,
        "quantum_factory": None,
    }
    preserve_callables = ["quantum_factory"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def engine_params(self):
        """
        :returns: (dict) of parameters needed to connect to engine without secrets.
        """
        return {"engine_url": "fake://example.com/abc"}

    def connect(self):
        pass

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    @property
    def data(self):
        return [{"fake": "data"}]

    @property
    def schema(self):
        return None

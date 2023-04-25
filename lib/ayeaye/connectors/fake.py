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

    @property
    def engine_params(self):
        """
        :returns: (dict) of parameters needed to connect to engine without secrets.
        """
        return {"engine_url": "fake://example.com/abc"}

    @property
    def data(self):
        return [{"fake": "data"}]

    @property
    def schema(self):
        return None

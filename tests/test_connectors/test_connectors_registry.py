import unittest

from ayeaye import Connect, connector_registry
from ayeaye.connectors.base import DataConnector
from ayeaye.exception import UnknownEngineType


class FakeConnector(DataConnector):
    engine_type = "superdata://"

    @property
    def data(self):
        return "fake data"


class TestConnectorsRegistry(unittest.TestCase):
    def tearDown(self):
        # ensure private connectors aren't left available outside of each test
        connector_registry.reset()

    def test_unknown_engine(self):
        "Connector not builtin to ayeaye and not registered"

        c = Connect(engine_url="magicdata://ref_to_data")
        with self.assertRaises(UnknownEngineType) as context:
            c.non_existant_attrib_loaded_on_demand

        self.assertEqual(
            "Unknown engine_type in url: 'magicdata://ref_to_data'", str(context.exception)
        )

    def test_non_public_engine(self):
        connector_registry.register_connector(FakeConnector)

        super_data = Connect(engine_url="superdata://ref_to_data")
        self.assertEqual("fake data", super_data.data)

    def test_valid_registrations(self):
        # should be a class
        fake_connector_obj = FakeConnector()
        with self.assertRaises(TypeError):
            connector_registry.register_connector(fake_connector_obj)

        # should be a subclass of DataConnector
        class X:
            pass

        with self.assertRaises(TypeError):
            connector_registry.register_connector(X)

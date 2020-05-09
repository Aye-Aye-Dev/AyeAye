import unittest

from ayeaye.ignition import Ignition, EngineUrlCase, EngineUrlStatus

EXAMPLE_ENGINE_URL_0 = "mysql://root:{env_secret_password}@localhost/my_database"


class TestIgnition(unittest.TestCase):

    def test_get_raw(self):
        """
        raw is always available because that's the url passed to constructor.
        """
        i = Ignition(EXAMPLE_ENGINE_URL_0)
        status, e_url = i.engine_url_at_state(EngineUrlCase.RAW)
        self.assertEqual(EngineUrlStatus.OK, status)
        self.assertEqual(EXAMPLE_ENGINE_URL_0, e_url)

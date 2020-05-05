import unittest

import ayeaye


class SimpleModel(ayeaye.Model):
    place_holder = "hello models"


class TestModelConnectors(unittest.TestCase):

    def test_single_standalone_model(self):
        c = ayeaye.Connect(models=SimpleModel)
        msg = ("Attrib access should be proxied through Connect to an instance of ModelsConnector "
               "which should refer back to the Connect instance that created it."
               )
        self.assertEqual(c, c._connect_instance, msg=msg)

import unittest

import ayeaye
from ayeaye.connectors.models_connector import ModelsConnector


class SimpleModel(ayeaye.Model):
    place_holder = "hello models"


class One(ayeaye.Model):
    pass


class Two(ayeaye.Model):
    pass


class Three(ayeaye.Model):
    pass


class TestModelConnectors(unittest.TestCase):

    def test_single_standalone_model(self):
        c = ayeaye.Connect(models=SimpleModel)
        msg = ("Attribute access should be proxied through Connect to an instance of "
               "ModelsConnector which should refer back to the Connect instance that created it."
               )
        self.assertEqual(c, c.connect_instance, msg=msg)
        self.assertEqual(c.models, [SimpleModel], "Single model should be proxied through Connect.")

    def test_construction(self):
        """
        valid ways to make a ModelsConnector instance.
        """
        m = ModelsConnector(models=One)
        self.assertIsInstance(m.models, list, "Single model becomes run-list")

        m = ModelsConnector(models=[One, Two, Three])
        self.assertIsInstance(m.models, list, "Preserves list")

        m = ModelsConnector(models=[One, Two, Three])
        self.assertIsInstance(m.models, list, "Preserves set")

        with self.assertRaises(ValueError):
            # non-model
            m = ModelsConnector(models=[One, Two, Three()])

        def models_choosen_at_runtime():
            return set([One, Two])

        with self.assertRaises(NotImplementedError):
            # TODO
            m = ModelsConnector(models=models_choosen_at_runtime)

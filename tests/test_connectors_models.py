import unittest

import ayeaye
from ayeaye.connectors.models_connector import ModelsConnector


class One(ayeaye.Model):
    a = ayeaye.Connect(engine_url="csv://a")
    b = ayeaye.Connect(engine_url="csv://b", access=ayeaye.AccessMode.WRITE)


class Two(ayeaye.Model):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    c = ayeaye.Connect(engine_url="csv://c", access=ayeaye.AccessMode.WRITE)


class Three(ayeaye.Model):
    c = Two.c.clone(access=ayeaye.AccessMode.READ)
    d = ayeaye.Connect(engine_url="csv://d", access=ayeaye.AccessMode.WRITE)


class TestModelConnectors(unittest.TestCase):

    def test_single_standalone_model(self):
        c = ayeaye.Connect(models=One)
        msg = ("Attribute access should be proxied through Connect to an instance of "
               "ModelsConnector which should refer back to the Connect instance that created it."
               )
        self.assertEqual(c, c.connect_instance, msg=msg)
        self.assertEqual(c.models, [One], "Single model should be proxied through Connect.")

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

    def test_resolve_run_order(self):
        """
        Dataset dependencies used to determine model run order.
        """
        c = ayeaye.Connect(models={One, Two, Three})
        r = c._resolve_run_order()

        leaf_sources = set([c.relayed_kwargs['engine_url'] for c in r.leaf_sources])
        expected_leaf_sources = {"csv://a"}
        self.assertEqual(expected_leaf_sources, leaf_sources)

        leaf_targets = set([c.relayed_kwargs['engine_url'] for c in r.leaf_targets])
        expected_leaf_targets = {"csv://d"}
        self.assertEqual(expected_leaf_targets, leaf_targets)

        msg = "Should be a single linear execution"
        self.assertIsInstance(r.run_order, list, msg)
        exec_node_names = []
        for exec_block in r.run_order:
            self.assertIsInstance(exec_block, set, msg)
            self.assertEqual(1, len(exec_block), msg)
            exec_node_names.append(list(exec_block)[0].model_name)

        self.assertEqual(['One', 'Two', 'Three'], exec_node_names, msg)

    def test_resolve_run_order_readwrite(self):
        # TODO
        pass

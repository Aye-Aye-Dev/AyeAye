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


class Four(ayeaye.Model):
    b_copy_paste = ayeaye.Connect(engine_url="csv://b", access=ayeaye.AccessMode.READ)
    e = ayeaye.Connect(engine_url="csv://e", access=ayeaye.AccessMode.WRITE)


class Five(ayeaye.Model):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    f = ayeaye.Connect(engine_url="sqlite:////data/f.db", access=ayeaye.AccessMode.READWRITE)


class Six(ayeaye.Model):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    f = Five.f.clone(access=ayeaye.AccessMode.WRITE)


class TestModelConnectors(unittest.TestCase):

    @staticmethod
    def repr_run_order(run_order):
        """
        @return: (list of sets) showing a simplified representation of the run_order from
        :method:`_resolve_run_order` using just the 'model_name' field.
        """
        r = []
        for task_group in run_order:
            assert isinstance(task_group, set)
            name_set = set([t.model_name for t in task_group])
            r.append(name_set)
        return r

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

    def test_resolve_run_order_linear(self):
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

        self.assertEqual([{'One'}, {'Two'}, {'Three'}], self.repr_run_order(r.run_order), msg)

    def test_resolve_run_order_one_branch(self):
        c = ayeaye.Connect(models={One, Two, Four})
        r = c._resolve_run_order()
        self.assertEqual([{'One'}, {'Two', 'Four'}], self.repr_run_order(r.run_order))

    def test_resolve_run_order_readwrite(self):
        c = ayeaye.Connect(models={One, Two, Five, Six})
        r = c._resolve_run_order()
        msg = ("There is an ambiguity because Six is WRITE and Five is READWRITE to the same "
               "dataset (f). The write only is happening first. Feels correct but might need "
               "more thought."
               )
        self.assertEqual([{'One'}, {'Two', 'Six'}, {'Five'}], self.repr_run_order(r.run_order), msg)

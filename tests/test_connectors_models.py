from inspect import isclass
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


def find_destination():
    """Will be called at build() time. In real life this would find out something that should
    only be looked up during runtime."""
    # return "csv://g.csv"
    msg = "The test shouldn't be calling this as the parent model class isn't instantiated"
    raise Exception(msg)


class Seven(ayeaye.Model):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    g = ayeaye.Connect(engine_url=find_destination, access=ayeaye.AccessMode.WRITE)


class Eight(ayeaye.Model):
    g = Seven.g.clone(access=ayeaye.AccessMode.READ)
    h = ayeaye.Connect(engine_url="csv://h", access=ayeaye.AccessMode.WRITE)


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

    @unittest.skip("needs fix for callable engine_url being called")
    def test_resolve_with_callable(self):
        "Seven has a callable to build it's engine_url at build time"
        c = ayeaye.Connect(models={One, Eight, Seven})
        r = c._resolve_run_order()
        self.assertEqual([{'One'}, {'Seven'}], self.repr_run_order(r.run_order))

    def test_model_iterator(self):
        c = ayeaye.Connect(models={One, Two, Five, Six})
        models = [m for m in c]
        # not ordered when a set of models is passed
        self.assertEqual(4, len(models))

        c = ayeaye.Connect(models=[One, Two, Five, Six])
        models = [m for m in c]
        # ordered when a list
        self.assertEqual([One, Two, Five, Six], models)

    def test_run_order(self):
        """
        run order is either specified when list is passed or resolved by examining dataset
        provenance but that is tested elsewhere.
        Here, check there is a public method that returns a list and elements within this list
        are a tree of lists and sets.
        """
        def is_run_item(r):
            if isinstance(r, list):
                # all items in list must be a set. Set could be one model
                for r2 in r:
                    assert isinstance(r2, set)
                    is_run_item(r2)
            elif isinstance(r, set):
                for r2 in r:
                    assert issubclass(r2, ayeaye.Model) or isinstance(r2, list)
                    if isinstance(r2, list):
                        is_run_item(r2)
            else:
                raise ValueError("Non list and not set item found")
            return True

        for c in [ayeaye.Connect(models={One, Two, Five, Six}),
                  ayeaye.Connect(models=[One, Two, Five, Six])
                  ]:
            run_order = c.run_order()
            self.assertIsInstance(run_order, list)
            self.assertTrue(is_run_item(run_order))

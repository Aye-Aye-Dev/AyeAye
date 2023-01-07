import unittest

import ayeaye
from ayeaye.model_collection import ModelCollection, ModelDataset, VisualiseModels


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


failed_callable_msg = "No test should be calling this as the parent model class isn't instantiated"


def find_destination():
    """Will be called at build() time. In real life this would find out something that should
    only be looked up during runtime."""
    # return "csv://g.csv"
    raise Exception(failed_callable_msg)


def another_find_destination():
    raise Exception(failed_callable_msg)


class Seven(ayeaye.Model):
    b = One.b.clone(access=ayeaye.AccessMode.READ)
    g = ayeaye.Connect(engine_url=find_destination, access=ayeaye.AccessMode.WRITE)


class Eight(ayeaye.Model):
    g = Seven.g.clone(access=ayeaye.AccessMode.READ)
    h = ayeaye.Connect(engine_url="csv://h", access=ayeaye.AccessMode.WRITE)


class Nine(ayeaye.Model):
    i = ayeaye.Connect(engine_url=another_find_destination, access=ayeaye.AccessMode.WRITE)
    h = ayeaye.Connect(engine_url="csv://h", access=ayeaye.AccessMode.WRITE)


class X(ayeaye.Model):
    r = ayeaye.Connect(engine_url="csv://r")
    s = ayeaye.Connect(engine_url="csv://s", access=ayeaye.AccessMode.WRITE)


class Y(ayeaye.Model):
    s = X.s.clone(access=ayeaye.AccessMode.READ)
    t = ayeaye.Connect(engine_url="csv://t", access=ayeaye.AccessMode.WRITE)


class Z(ayeaye.Model):
    t = Y.t.clone(access=ayeaye.AccessMode.READ)
    u = ayeaye.Connect(engine_url="csv://u", access=ayeaye.AccessMode.WRITE)


class TestModelCollection(unittest.TestCase):
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

    def test_construction(self):
        """
        valid ways to make a ModelCollection instance.
        """
        m = ModelCollection(One)
        self.assertIsInstance(m.models, list, "Single model becomes run-list")

        m = ModelCollection([One, Two, Three])
        self.assertIsInstance(m.models, list, "Preserves list")

        m = ModelCollection(set([One, Two, Three]))
        self.assertIsInstance(m.models, set, "Preserves set")

        with self.assertRaises(ValueError):
            # non-model
            m = ModelCollection([One, Two, Three()])

        def models_choosen_at_runtime():
            return set([One, Two])

        with self.assertRaises(NotImplementedError):
            # TODO
            m = ModelCollection(models_choosen_at_runtime)

    def test_resolve_run_order_linear(self):
        """
        Dataset dependencies used to determine model run order.
        """
        c = ModelCollection(models={One, Two, Three})
        r = c._resolve_run_order()

        leaf_sources = set([c.connector.relayed_kwargs["engine_url"] for c in r.leaf_sources])
        expected_leaf_sources = {"csv://a"}
        self.assertEqual(expected_leaf_sources, leaf_sources)

        leaf_targets = set([c.connector.relayed_kwargs["engine_url"] for c in r.leaf_targets])
        expected_leaf_targets = {"csv://d"}
        self.assertEqual(expected_leaf_targets, leaf_targets)

        msg = "Should be a single linear execution"
        self.assertIsInstance(r.run_order, list, msg)

        self.assertEqual([{"One"}, {"Two"}, {"Three"}], self.repr_run_order(r.run_order), msg)

    def test_resolve_run_order_one_branch(self):
        c = ModelCollection(models={One, Two, Four})
        r = c._resolve_run_order()
        self.assertEqual([{"One"}, {"Two", "Four"}], self.repr_run_order(r.run_order))

    def test_resolve_run_order_readwrite(self):
        c = ModelCollection(models={One, Two, Five, Six})
        r = c._resolve_run_order()
        msg = (
            "There is an ambiguity because Six is WRITE and Five is READWRITE to the same "
            "dataset (f). The write only is happening first. Feels correct but might need "
            "more thought."
        )
        self.assertEqual([{"One"}, {"Two", "Six"}, {"Five"}], self.repr_run_order(r.run_order), msg)

    def test_resolve_with_callable(self):
        "Seven has a callable to build it's engine_url at build time"
        c = ModelCollection(models={One, Eight, Seven})
        r = c._resolve_run_order()
        self.assertEqual([{"One"}, {"Seven"}, {"Eight"}], self.repr_run_order(r.run_order))

    def test_resolve_with_two_different_callables(self):
        c = ModelCollection(models={One, Nine, Seven})
        r = c._resolve_run_order()
        self.assertEqual([{"One", "Nine"}, {"Seven"}], self.repr_run_order(r.run_order))

    def test_model_iterator(self):
        c = ModelCollection(models={One, Two, Five, Six})
        models = [m for m in c]
        # not ordered when a set of models is passed
        self.assertEqual(4, len(models))

        c = ModelCollection(models=[One, Two, Five, Six])
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

        for c in [
            ModelCollection(models={One, Two, Five, Six}),
            ModelCollection(models=[One, Two, Five, Six]),
        ]:
            run_order = c.run_order()
            self.assertIsInstance(run_order, list)
            self.assertTrue(is_run_item(run_order))

    def test_data_provenance_model_classes(self):
        """
        without instanciating ayeaye.Model classes find data provenance (aka data lineage)
        """
        c = ModelCollection(models={One, Two, Three})
        dataset_graphs = c.dataset_provenance()

        msg = "{One,Two,Three} use datasets (a,b,c,d} so are all inter-related into single graph"
        self.assertEqual(1, len(dataset_graphs), msg)

        msg = "There are 4 datasets so there should be 4 edges"
        graph_edges = dataset_graphs[0]
        self.assertEqual(4, len(graph_edges), msg)

        models = set()
        for edge in graph_edges:
            models.add(edge.model_a)
            models.add(edge.model_b)

        msg = "None indicates leaf node"
        self.assertIn(None, models, msg)
        models.remove(None)

        msg = "There are 3 models"
        self.assertEqual(3, len(models), msg)

    @unittest.skip("TODO: Incomplete provenance code")
    def test_data_provenance_multiple_graphs(self):
        """
        The set of models contains two separate (none-connected) graphs.
        """
        c = ModelCollection(models={One, Two, Three, X, Y, Z})
        dataset_graphs = c.dataset_provenance()

        msg = "{One,Two,Three} and {X,Y,Z} are separate graphs"
        self.assertEqual(2, len(dataset_graphs), msg)

        ordered_models = set()
        for graph in dataset_graphs:
            models = set()
            for graph_edge in graph:
                for edge_model in [graph_edge.model_a, graph_edge.model_b]:
                    models.add(edge_model.__name__ if edge_model is not None else "Leaf")

            models = list(models)
            models.sort()
            ordered_models.add(",".join(models))

        msg = "Models should be grouped into graphs. Nodes are the models."
        expected_models = {"Leaf,X,Y,Z", "Leaf,One,Three,Two"}
        self.assertEqual(expected_models, ordered_models, msg)

    def test_modeldataset(self):
        """
        ModelDataset should act as a proxy to ayeaye.Connect when comparisons are made.
        """

        c0 = ayeaye.Connect(engine_url="csv://a0", access=ayeaye.AccessMode.READ)
        c1 = ayeaye.Connect(engine_url="csv://a0", access=ayeaye.AccessMode.WRITE)
        self.assertEqual(c0, c1)

        md_0 = ModelDataset(model_attrib_label="a0_read", connector=c0)
        md_1 = ModelDataset(model_attrib_label="a0_write", connector=c1)
        msg = "Different attrib label and write mode shouldn't matter, these are the same dataset"
        self.assertEqual(md_0, md_1, msg)

    def test_mermaid_run_order(self):
        "Visualisation experiment - incomplete"

        c = ModelCollection(models={One, Two, Three})
        visual = VisualiseModels(model_collection=c)
        mermaid_content = visual.mermaid_data_provenance()

        self.assertIn("One-->|b| Two", mermaid_content)

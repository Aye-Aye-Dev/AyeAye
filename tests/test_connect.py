import os
import unittest

from ayeaye import AccessMode
from ayeaye.connect import Connect
from ayeaye.connectors.csv_connector import CsvConnector, TsvConnector
from ayeaye.connectors.fake import FakeDataConnector
from ayeaye.connectors.sqlalchemy_database import SqlAlchemyDatabaseConnector

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_CSV_PATH = os.path.join(PROJECT_TEST_PATH, "data", "deadly_creatures.csv")
EXAMPLE_TSV_PATH = os.path.join(PROJECT_TEST_PATH, "data", "monkeys.tsv")


class AbstractFakeModel:
    def __init__(self):
        self._connections = {}


class TestConnect(unittest.TestCase):
    def test_connect_standalone(self):
        """
        :class:`ayeaye.Connect` can be used outside of the ETL so data discovery can use the same
        way of working as full :class:`ayeaye.Model`s.
        """
        # happy path
        # it works without Connect being part of a ayeaye.Model
        c = Connect(engine_url="fake://MyDataset")
        self.assertEqual({"fake": "data"}, c.data[0])

    def test_connect_spare_kwargs(self):
        """
        subclasses of :class:`ayeaye.connectors.base.DataConnector` can be given specific/custom
        kwargs. An exception should be raised when unclaimed spare kwargs remain. This will make
        it harder for users to make mistakes and typos referring to arguments that never come
        into play.
        """
        c = Connect(engine_url="fake://foo", doesntexist="oh dear")
        with self.assertRaises(ValueError):
            # the kwargs are not used until an engine_url is needed
            c._prepare_connection()

    def test_connect_within_instantiated_class(self):
        """
        Connect used as a class variable. The parent class, which in practice will be a
        :class:`ayeaye.Model`.
        When used as a class variable in an instantiated class, Connect() will store information
        about the dataset within the parent (i.e. Model) class.
        """

        class FakeModel(AbstractFakeModel):
            insects = Connect(engine_url="fake://bugsDB")

        e0 = FakeModel()
        self.assertEqual(0, len(e0._connections))

        # connect on demand/access
        self.assertIsNotNone(e0.insects)
        self.assertEqual(1, len(e0._connections))

    def test_custom_kwargs_are_passed(self):
        """
        ayeaye.Connect should relay kwargs to subclasses of DataConnecter
        """
        # using bigquery because it has custom 'credentials' kwarg
        engine_url = "bigquery://projectId=my_project;datasetId=nice_food;tableId=cakes;"
        c = Connect(engine_url=engine_url, credentials="hello_world")
        # on demand connection
        self.assertIsNotNone(c.data)
        self.assertEqual("hello_world", c._standalone_connection.credentials)

    def test_overlay_args(self):
        """
        Make an access=AccessMode.READ connection in a model into access=AccessMode.WRITE.
        The engine_url stays the same.
        """

        class FakeModel(AbstractFakeModel):
            insects = Connect(engine_url="fake://bugsDB")

        class FakeModelWrite:
            insects = FakeModel.insects.clone(access=AccessMode.WRITE)

            def __init__(self):
                self._connections = {}

        f = FakeModelWrite()
        self.assertEqual("fake://bugsDB", f.insects.engine_url)
        self.assertEqual(AccessMode.WRITE, f.insects.access)

    def test_replace_existing_connect(self):
        class FakeModel(AbstractFakeModel):
            insects = Connect(engine_url="fake://bugsDB")

        m = FakeModel()
        with self.assertRaises(ValueError) as context:
            m.insects = "this is a string, not an instance of Connect"
        self.assertEqual("Only Connect instances can be set", str(context.exception))

        self.assertEqual({}, m._connections, "Connections not initialised prior to access")
        self.assertEqual("fake://bugsDB", m.insects.engine_url, "Original connection")

        m.insects = Connect(engine_url="fake://creepyCrawliesDB")
        self.assertEqual("fake://creepyCrawliesDB", m.insects.engine_url, "New connection")

    def test_update_by_replacement(self):
        """
        Take a connection from a model, make a small tweak and set it back into the model.
        Note is isn't a class tweak (that is tested elsewhere), it's on instances.
        """

        class FakeModel(AbstractFakeModel):
            insects = Connect(engine_url="fake://bugsDB")

        m = FakeModel()
        self.assertTrue(AccessMode.READ == m.insects.access, "Expected starting state not found")

        connect_refs = [k for k in m._connections.keys()]

        # change something, this could have been more dramatic, the engine type for example
        # Assigning this back to insects will re-prepare the connection
        c = m.insects.connect_instance
        c.update(access=AccessMode.WRITE)
        m.insects = c

        self.assertEqual(AccessMode.WRITE, m.insects.access, "Change to connection went missing")
        connect_refs_now = [k for k in m._connections.keys()]
        self.assertEqual(connect_refs, connect_refs_now, "Connect instances shouldn't change")

    def test_update_inline(self):
        """
        A change to the `Connect` propagates to build a new DataConnector with a different type.
        """

        class FakeModel(AbstractFakeModel):
            insects = Connect(engine_url="fake://bugsDB")

        m = FakeModel()
        self.assertIsInstance(m.insects, FakeDataConnector)

        m.insects.update(engine_url="mysql://")
        self.assertIsInstance(m.insects, SqlAlchemyDatabaseConnector)

    def test_compile_time_multiple_engine_urls(self):
        """
        engine_url could be a list of engine_urls.
        In the future, a dictionary version might be added
        """
        tsv_engine_url = "tsv://" + EXAMPLE_TSV_PATH
        csv_engine_url = "csv://" + EXAMPLE_CSV_PATH
        c = Connect(engine_url=[tsv_engine_url, csv_engine_url])

        all_the_animals = []
        for index, data_connector in enumerate(c):
            if index == 0:
                self.assertIsInstance(data_connector, TsvConnector)
            elif index == 1:
                self.assertIsInstance(data_connector, CsvConnector)
            else:
                raise ValueError("Connect has more than expected data connectors")

            all_the_animals += [animal.common_name for animal in data_connector]

        expected = [
            "Goeldi's marmoset",
            "Common squirrel monkey",
            "Crab-eating macaque",
            "Crown of thorns starfish",
            "Golden dart frog",
        ]
        self.assertEqual(expected, all_the_animals)

    def test_compile_time_multiple_engine_urls_in_a_model(self):
        """
        Check the descriptors are treating children of MultiConnector the same as
        the other DataConnector subclasses.
        """

        class AnimalsModel(AbstractFakeModel):
            animals = Connect(engine_url=["tsv://" + EXAMPLE_TSV_PATH, "csv://" + EXAMPLE_CSV_PATH])

        m = AnimalsModel()

        all_the_animals = []
        for animal_dataset in m.animals:
            all_the_animals += [animal.common_name for animal in animal_dataset]

        expected = [
            "Goeldi's marmoset",
            "Common squirrel monkey",
            "Crab-eating macaque",
            "Crown of thorns starfish",
            "Golden dart frog",
        ]
        self.assertEqual(expected, all_the_animals)

    def test_callable_engine_url(self):
        def pointlessly_deterministic_example_callable():
            return "fake://MyDataset"

        c = Connect(engine_url=pointlessly_deterministic_example_callable)
        self.assertEqual({"fake": "data"}, c.data[0], "Example data not found")

    def test_split_instance_and_connect(self):
        """
        Connect() is only called when the class is imported. If the instance of Connect is mutated
        this shouldn't be passed to other instances.
        Bug found in other project.
        """

        class AnimalsModel(AbstractFakeModel):
            animals = Connect(engine_url=[])

        m1 = AnimalsModel()
        m1.animals.engine_url.append("hi")

        m2 = AnimalsModel()
        self.assertEqual([], m2.animals.engine_url)

    def test_steal_model_connect(self):
        """
        Take a Connect from a model's class variables and use it as a standalone Connect.
        """

        class AnimalsModel(AbstractFakeModel):
            animals = Connect(engine_url="csv://" + EXAMPLE_CSV_PATH)

        animals = AnimalsModel.animals
        self.assertEqual("ConnectBind.NEW", str(animals.connection_bind))

        all_the_animals = [animal.common_name for animal in animals]

        expected = ["Crown of thorns starfish", "Golden dart frog"]
        self.assertEqual(expected, all_the_animals)

        self.assertEqual("ConnectBind.STANDALONE", str(animals.connection_bind))

    def test_standalone_in_non_model_instance(self):
        """
        Make changes to a Connect that doesn't belong to an ayeaye.Model.
        Also see :meth:`TestModels.test_double_usage` for how this behaves with an ayeaye.Model.
        """

        class NonModelClass:
            "doesn't have self._connections"

            def __init__(self):
                # the instance of Connect is just a variable, it's not an attribute so it's descriptor
                # methods aren't called.
                self.animals = Connect(engine_url="csv://" + EXAMPLE_CSV_PATH)

            def go(self):
                all_the_animals = [animal.common_name for animal in self.animals]
                return all_the_animals

        nmc = NonModelClass()
        self.assertEqual(["Crown of thorns starfish", "Golden dart frog"], nmc.go())

        nmc.animals.update(engine_url="tsv://" + EXAMPLE_TSV_PATH)
        expected_values = ["Goeldi's marmoset", "Common squirrel monkey", "Crab-eating macaque"]
        self.assertEqual(expected_values, nmc.go())

    def test_standalone_as_proxy(self):
        """
        Access an attribute of the subclass that doesn't belong to the DataConnector abstract class.
        """
        animals = Connect(engine_url="csv://" + EXAMPLE_CSV_PATH + ";encoding=magic_encoding")
        self.assertEqual("magic_encoding", animals.encoding)

    def test_construction_args(self):
        with self.assertRaises(ValueError, msg="Ref and engine_url are mutually exclusive"):
            Connect(ref="x", engine_url="tsv://" + EXAMPLE_TSV_PATH)

    def test_connect_callable_kwargs(self):
        """
        :class:`ayeaye.connectots.fake.FakeDataConnector` has an optional kwarg-
        'quantum_accelerator_module' set this using a literal or a callable.
        """

        c = Connect(engine_url="fake://MyDataset", quantum_accelerator_module="entanglement_v1")
        self.assertEqual({"fake": "data"}, c.data[0])
        self.assertEqual("entanglement_v1", c.quantum_accelerator_module)

        def simple_callable():
            "simple means it doesn't take arguments"
            return "entanglement_v2"

        # TODO - standalone is only calling the callable after _prepare_connection
        # this isn't right
        # c = Connect(engine_url="fake://MyDataset", quantum_accelerator_module=simple_callable)
        # self.assertEqual({"fake": "data"}, c.data[0])

        class QuatumSort(AbstractFakeModel):
            source = Connect(
                engine_url="fake://MyDataset", quantum_accelerator_module=simple_callable
            )

        m1 = QuatumSort()
        self.assertEqual("entanglement_v2", m1.source.quantum_accelerator_module)

    def test_connect_preserve_callable_kwargs(self):
        """
        :class:`ayeaye.connectots.fake.FakeDataConnector` has an optional kwarg-
        'quantum_accelerator_module' set this using a literal or a callable.
        The 'quantum_factory' optional arg shouldn't be called by ayeaye.Connect and should still
        be a callable when used to initiate the FakeDataConnector.
        """

        def q_fact(my_thing):
            return "hello " + str(my_thing)

        class QuatumSort(AbstractFakeModel):
            source = Connect(engine_url="fake://MyDataset", quantum_factory=q_fact)

            def calculate_result(self):
                return self.source.quantum_factory("quantum dynamics")

        m1 = QuatumSort()
        self.assertTrue(callable(m1.source.quantum_factory))
        self.assertEqual("hello quantum dynamics", m1.calculate_result())

    def test_method_overlay(self):
        """
        Add methods to a connector at runtime without inheritance.
        """

        def field_name_aliases(self):
            """
            vanity iterator to alias the 'common_name' field to 'animal_name'
            """
            for r in self:
                r.animal_name = r.common_name
                yield r

        class AnimalsModel(AbstractFakeModel):
            animals = Connect(
                engine_url="csv://" + EXAMPLE_CSV_PATH,
                method_overlay=field_name_aliases,
            )

        m = AnimalsModel()

        # note the use of `field_name_aliases` and `animal_name`
        all_the_animals = [animal.animal_name for animal in m.animals.field_name_aliases()]

        expected = ["Crown of thorns starfish", "Golden dart frog"]
        self.assertEqual(expected, all_the_animals)

    def test_method_overlay_multi_connector(self):
        """
        A method added to a multi connector should behave just like one added to a single
        datasource connector. The method shouldn't be available to the child connectors.
        """

        def fake_method(self):
            return "multi-connector-overlay"

        class AnimalsModel(AbstractFakeModel):
            animals = Connect(
                engine_url=[
                    "csv://" + EXAMPLE_CSV_PATH,
                    "csv://" + EXAMPLE_TSV_PATH,
                ],
                method_overlay=fake_method,
            )

        m = AnimalsModel()

        msg = "method should be available on multi-connector but not children"
        self.assertEqual("multi-connector-overlay", m.animals.fake_method(), msg)

        for child_connector in m.animals:
            with self.assertRaises(AttributeError) as c:
                child_connector.fake_method()

            self.assertIn("object has no attribute 'fake_method'", str(c.exception), msg)

    def test_method_overlay_multi_connector_children(self):
        """
        It should be possible to pass methods to the child connectors of a multi-connector. The method
        won't be available to the parent multi-connector.
        """

        def fake_method(self):
            return "multi-connector-overlay"

        class AnimalsModel(AbstractFakeModel):
            animals = Connect(
                engine_url=[
                    "csv://" + EXAMPLE_CSV_PATH,
                    "csv://" + EXAMPLE_TSV_PATH,
                ],
                child_method_overlay=fake_method,
            )

        m = AnimalsModel()

        msg = "method shouldn't be available on multi-connector but is in children"

        with self.assertRaises(AttributeError) as c:
            m.animals.fake_method()

        self.assertIn("object has no attribute 'fake_method'", str(c.exception), msg)

        for child_connector in m.animals:
            self.assertEqual("multi-connector-overlay", child_connector.fake_method(), msg)

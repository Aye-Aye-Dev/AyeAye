import os
import unittest

from ayeaye import AccessMode
from ayeaye.connect import Connect
from ayeaye.connect_resolve import connector_resolver
from ayeaye.connectors.csv_connector import CsvConnector, TsvConnector
from ayeaye.connectors.fake import FakeDataConnector
from ayeaye.connectors.sqlalchemy_database import SqlAlchemyDatabaseConnector

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_CSV_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'deadly_creatures.csv')
EXAMPLE_TSV_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'monkeys.tsv')


class FakeModel:
    insects = Connect(engine_url="fake://bugsDB")

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
        assert c.data[0] == {'fake': 'data'}

    def test_connect_spare_kwargs(self):
        """
        subclasses of :class:`ayeaye.connectors.base.DataConnector` can be given specific/custom
        kwargs. An exception should be raised when unclaimed spare kwargs remain. This will make
        it harder for users to make mistakes and typos referring to arguments that never come
        into play.
        """
        c = Connect(engine_url="fake://foo", doesntexist='oh dear')
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
        e0 = FakeModel()
        assert len(e0._connections) == 0

        # connect on demand/access
        assert e0.insects is not None
        assert len(e0._connections) == 1

    def test_connect_within_class(self):
        """
        Connect used as a class variable. On access it returns a new instance that is separated,
        i.e. not the same object as, the original.
        """
        copy_0 = FakeModel.insects
        copy_1 = FakeModel.insects

        assert id(copy_0) != id(copy_1)

    def test_custom_kwargs_are_passed(self):
        """
        ayeaye.Connect should relay kwargs to subclasses of DataConnecter
        """
        # using bigquery because it has custom 'credentials' kwarg
        engine_url = 'bigquery://projectId=my_project;datasetId=nice_food;tableId=cakes;'
        c = Connect(engine_url=engine_url, credentials="hello_world")
        # on demand connection
        assert c.data is not None
        assert c._local_dataset.credentials == "hello_world"

    def test_overlay_args(self):
        """
        Make an access=AccessMode.READ connection in a model into access=AccessMode.WRITE.
        The engine_url stays the same.
        """
        class FakeModelWrite:
            insects = FakeModel.insects(access=AccessMode.WRITE)

            def __init__(self):
                self._connections = {}

        f = FakeModelWrite()
        self.assertEqual('fake://bugsDB', f.insects.engine_url)
        self.assertEqual(AccessMode.WRITE, f.insects.access)

    def test_replace_existing_connect(self):

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
        m = FakeModel()
        self.assertTrue(AccessMode.READ == m.insects.access, "Expected starting state not found")

        connect = m.insects.connect_instance
        connect_refs = [k for k in m._connections.keys()]

        # change something, this could have been more dramatic, the engine type for example
        connect(access=AccessMode.WRITE)

        # this set will re-prepare the connection
        m.insects = connect

        self.assertTrue(AccessMode.WRITE == m.insects.access, "Change to connection went missing")
        connect_refs_now = [k for k in m._connections.keys()]
        self.assertEqual(connect_refs, connect_refs_now, "Connect instances shouldn't change")

    def test_update_inline(self):
        """
        A change to the `Connect` propagates to build a new DataConnector with a different type.
        """
        m = FakeModel()
        self.assertIsInstance(m.insects, FakeDataConnector)

        m.insects(engine_url="mysql://")
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

        expected = ['Goeldi\'s marmoset',
                    'Common squirrel monkey',
                    'Crab-eating macaque',
                    'Crown of thorns starfish',
                    'Golden dart frog',
                    ]
        self.assertEqual(expected, all_the_animals)

    def test_compile_time_multiple_engine_urls_in_a_model(self):
        """
        Check the descriptors are treating children of MultiConnector the same as
        the other DataConnector subclasses.
        """
        class AnimalsModel(FakeModel):
            animals = Connect(engine_url=["tsv://" + EXAMPLE_TSV_PATH, "csv://" + EXAMPLE_CSV_PATH])

        m = AnimalsModel()

        all_the_animals = []
        for animal_dataset in m.animals:
            all_the_animals += [animal.common_name for animal in animal_dataset]

        expected = ['Goeldi\'s marmoset',
                    'Common squirrel monkey',
                    'Crab-eating macaque',
                    'Crown of thorns starfish',
                    'Golden dart frog',
                    ]
        self.assertEqual(expected, all_the_animals)

    def test_multi_connector_resolve(self):
        """
        MultiConnector + ConnectorResolver.
        Other tests for this in :class:`TestConnectors`.
        """

        def simple_resolver(unresolved_engine_url):
            return unresolved_engine_url.format(**{'data_version': '1234'})

        # A MultiConnector
        c = Connect(engine_url=["csv://my_path_x/data_{data_version}.csv",
                                "csv://my_path_y/data_{data_version}.csv"
                                ]
                    )

        with connector_resolver.context(simple_resolver):
            resolved_engine_urls = [data_conn.engine_url for data_conn in c]

        expected_urls = ['csv://my_path_x/data_1234.csv', 'csv://my_path_y/data_1234.csv']
        self.assertEqual(expected_urls, resolved_engine_urls)

import os
import tarfile
import tempfile
import unittest

import ayeaye
from ayeaye.connect_resolve import connector_resolver
from ayeaye.connectors.flowerpot import FlowerpotEngine, FlowerPotConnector
from ayeaye.connectors.csv_connector import CsvConnector, TsvConnector
from ayeaye.connectors.multi_connector import MultiConnector

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_FLOWERPOT_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'exampleflowerpot.tar.gz')
EXAMPLE_CSV_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'deadly_creatures.csv')
EXAMPLE_TSV_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'monkeys.tsv')
EXAMPLE_ENGINE_URL = 'gs+flowerpot://fake_flowerpot_bucket/some_file.json'


class TestConnectors(unittest.TestCase):

    def test_flowerpot_deserialize(self):
        test_string = bytes(
            '{"availability": "apple", "referential": "raspberry"}\n{"availability": "anchor", "referential": "rudder"}',
            encoding='utf-8')
        result = FlowerpotEngine._deserialize_ndjson_string(test_string)
        assert result[0] == {"availability": "apple", "referential": "raspberry"}
        assert result[1] == {"availability": "anchor", "referential": "rudder"}

    def test_iterate_over_json_lines(self):
        with tarfile.open(EXAMPLE_FLOWERPOT_PATH, 'r:gz') as tf:
            reader = FlowerpotEngine(tf)
            results = list(reader.items())
            assert len(results) == 4
            assert "availability" in results[0]
            assert "referential" in results[0]

    def test_flowerpot_all_items(self):
        """
        Iterate all the data items in all the files in the example flowerpot.
        """
        c = FlowerPotConnector(engine_url="flowerpot://" + EXAMPLE_FLOWERPOT_PATH)
        all_items = [(r.availability, r.referential) for r in c]
        all_items.sort()
        expected = "[('acoustic', 'rap'), ('anchor', 'rudder'), ('antenna', 'receive'), ('apple', 'raspberry')]"
        assert expected == str(all_items)

    def test_flowerpot_query_one_file(self):
        """
        The 'table' kwarg gets rows from all files that start with that string.
        """
        c = FlowerPotConnector(engine_url="flowerpot://" + EXAMPLE_FLOWERPOT_PATH)
        some_items = [(r.availability, r.referential) for r in c.query(table='test_a')]
        some_items.sort()
        expected = "[('anchor', 'rudder'), ('apple', 'raspberry')]"
        assert expected == str(some_items)

    def test_csv_basics(self):
        """
        Iterate all the data items and check each row is being yielded as an instance of
        :class:`ayeaye.Pinnate`
        """
        c = CsvConnector(engine_url="csv://" + EXAMPLE_CSV_PATH)

        animals_names = ", ".join([deadly_animal.common_name for deadly_animal in c])
        expected = 'Crown of thorns starfish, Golden dart frog'
        assert expected == animals_names

    def test_csv_write(self):
        """
        Write to a CSV without using a schema.
        """
        data_dir = tempfile.mkdtemp()
        csv_file = os.path.join(data_dir, "fish.csv")
        c = CsvConnector(engine_url="csv://" + csv_file, access=ayeaye.AccessMode.WRITE)

        # two data types that can be added
        p = ayeaye.Pinnate({'common_name': 'Angel fish'})
        c.add(p)

        d = {'common_name': 'Grey reef shark'}
        c.add(d)

        c = None  # flush to disk on deconstruction

        with open(csv_file, 'r') as f:
            csv_content = f.read()

        expected_content = ('common_name\n'
                            'Angel fish\n'
                            'Grey reef shark\n'
                            )

        self.assertEqual(expected_content, csv_content)

    def test_tsv_basics(self):
        """
        Tab separated, Iterate all the data items and check each row is being yielded as an
        instance of :class:`ayeaye.Pinnate`
        """
        c = TsvConnector(engine_url="tsv://" + EXAMPLE_TSV_PATH)

        monkey_names = ", ".join([monkey.common_name for monkey in c])
        expected = "Goeldi's marmoset, Common squirrel monkey, Crab-eating macaque"
        assert expected == monkey_names

    def test_resolve_engine_url(self):
        """
        The engine_url contains a parameter that is replaced on demand.
        """
        msg = "There are existing resolver callables before the test has started"
        self.assertEqual(0, len(connector_resolver.resolver_callables), msg)

        class MockFakeEngineResolver:
            "Record when it's used and just substitute {data_version} with '1234'"

            def __init__(self):
                self.has_been_called = False

            def __call__(self, unresolved_engine_url):
                self.has_been_called = True
                return unresolved_engine_url.format(**{'data_version': '1234'})

        c = CsvConnector(engine_url="csv://my_path/data_{data_version}.csv")

        m_resolver = MockFakeEngineResolver()
        with connector_resolver.context(m_resolver):
            self.assertFalse(m_resolver.has_been_called, "Should only be called on demand")
            msg = "One resolver exists during the .context"
            self.assertEqual(1, len(connector_resolver.resolver_callables), msg)

            self.assertEqual('csv://my_path/data_1234.csv', c.engine_url)

            msg = "Should have been called after engine_url is available"
            self.assertTrue(m_resolver.has_been_called, msg)

        msg = "At end of with .context the MockFakeEngineResolver should have been removed"
        self.assertEqual(0, len(connector_resolver.resolver_callables), msg)

    def test_multi_connector_append(self):
        """
        Add engine_urls at runtime.
        This is a preemptive measure for when the post build lock file (not yet implemented) will
        store runtime changes to the data connections.
        """
        c = MultiConnector(engine_url=['csv://data_1234.csv', 'csv://data_4567.csv'],
                           access=ayeaye.AccessMode.WRITE,
                           )
        c.engine_url.append('csv://data_8910.csv')

        all_urls = [connector.engine_url for connector in c]
        expected_urls = ['csv://data_1234.csv', 'csv://data_4567.csv', 'csv://data_8910.csv']
        self.assertEqual(expected_urls, all_urls)

        # check that late additions to c.engine_url are visible. 'late' means after c.connect()
        # has been called
        another_file = 'csv://data_1112.csv'
        c.engine_url.append(another_file)
        all_urls = [connector.engine_url for connector in c]
        expected_urls.append(another_file)
        self.assertEqual(expected_urls, all_urls)

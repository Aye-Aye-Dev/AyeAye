import os
import tarfile
import tempfile
import unittest

import ayeaye
from ayeaye.connectors.flowerpot import FlowerpotEngine, FlowerPotConnector
from ayeaye.connectors.json_connector import JsonConnector
from ayeaye.connectors.multi_connector import MultiConnector

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_FLOWERPOT_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'exampleflowerpot.tar.gz')
EXAMPLE_ENGINE_URL = 'gs+flowerpot://fake_flowerpot_bucket/some_file.json'
EXAMPLE_JSON_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'london_weather.json')
EXAMPLE_CSV_MICE = os.path.join(PROJECT_TEST_PATH, 'data', 'mice.csv')
EXAMPLE_CSV_SQUIRRELS = os.path.join(PROJECT_TEST_PATH, 'data', 'squirrels.csv')


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

    def test_json_basics(self):
        c = JsonConnector(engine_url="json://" + EXAMPLE_JSON_PATH)
        self.assertEqual('London', c.data.name)
        self.assertEqual('light intensity drizzle', c.data.weather.description)

    def test_json_write(self):

        data_dir = tempfile.mkdtemp()
        json_file = os.path.join(data_dir, "chips.json")
        c = JsonConnector(engine_url=f"json://{json_file}", access=ayeaye.AccessMode.WRITE)

        good_examples = [('a string', '"a string"'),
                         (ayeaye.Pinnate({'a': 1}), '{"a": 1}'),
                         ({'a': 1}, '{"a": 1}'),
                         ([1, 2, 3], '[1, 2, 3]')
                         ]

        for acceptable_data, expected_json in good_examples:

            c.data = acceptable_data
            with open(json_file, 'r', encoding=c.encoding) as f:
                json_not_decoded = f.read()
                self.assertEqual(expected_json, json_not_decoded)

        # these data types can't be encoded as JSON
        bad_examples = [set([1, 2, 3]),
                        c,
                        ]
        for unacceptable_data in bad_examples:
            with self.assertRaises(TypeError):
                c.data = unacceptable_data


    def test_multi_connector_passes_args(self):
        """
        kwargs given to Connect should be passed to each DataConnection created by multi-connect.
        """
        # header-less CSVs
        c = MultiConnector(engine_url=["csv://" + EXAMPLE_CSV_MICE, "csv://" + EXAMPLE_CSV_SQUIRRELS],
                           access=ayeaye.AccessMode.READ,
                           field_names=['common_name', 'scientific_name', 'geo_distribution']
                           )
        rodents = []
        for cx in c:
            rodents.extend([r.as_dict() for r in cx])

        expected_line_0 = {'common_name': 'Yellow-necked mouse',
                           'scientific_name': 'Apodemus flavicollis',
                           'geo_distribution': 'Europe'
                           }

        expected_line_3 = {'common_name': 'American red squirrel',
                           'scientific_name': 'Tamiasciurus hudsonicus',
                           'geo_distribution': 'North America'
                           }

        self.assertEqual(5, len(rodents))
        self.assertEqual(expected_line_0, rodents[0])
        self.assertEqual(expected_line_3, rodents[3])

    def test_json_datasource_exists(self):
        c = JsonConnector(engine_url="json://" + EXAMPLE_JSON_PATH)
        self.assertTrue(c.datasource_exists)

        c = JsonConnector(engine_url="json://this_doesnt_exist.json")
        self.assertFalse(c.datasource_exists)

    def test_multi_connector_by_engine_url(self):
        """
        Get a data connection from a multi connector by engine_url
        """
        engine_0 = "csv://" + EXAMPLE_CSV_MICE
        engine_1 = "csv://" + EXAMPLE_CSV_SQUIRRELS
        c = MultiConnector(engine_url=[engine_0, engine_1],
                           access=ayeaye.AccessMode.READ,
                           field_names=['common_name', 'scientific_name', 'geo_distribution']
                           )

        dataset = c[engine_0]
        # check access to any dataset property
        self.assertTrue(dataset.engine_params.file_path.endswith('tests/data/mice.csv'))
        self.assertEqual(2, len(c))


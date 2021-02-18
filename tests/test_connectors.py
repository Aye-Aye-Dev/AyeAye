import os
import tarfile
import tempfile
import unittest

import ayeaye
from ayeaye.connectors.flowerpot import FlowerpotEngine, FlowerPotConnector
from ayeaye.connectors.csv_connector import CsvConnector, TsvConnector
from ayeaye.connectors.json_connector import JsonConnector
from ayeaye.connectors.multi_connector import MultiConnector

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_FLOWERPOT_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'exampleflowerpot.tar.gz')
EXAMPLE_CSV_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'deadly_creatures.csv')
EXAMPLE_TSV_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'monkeys.tsv')
EXAMPLE_ENGINE_URL = 'gs+flowerpot://fake_flowerpot_bucket/some_file.json'
EXAMPLE_JSON_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'london_weather.json')
EXAMPLE_CSV_BROKEN_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'deadly_missing_values.csv')
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
        c.close_connection()  # flush to disk

        with open(csv_file, 'r', encoding=c.encoding) as f:
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

    def test_csv_encoding(self):
        """
        Specify character encoding in the URL. This test doesn't ensure data conforms.
        """
        c = CsvConnector(engine_url="csv://" + EXAMPLE_CSV_PATH)
        self.assertEqual("utf-8-sig", c.encoding, "Unexpected default encoding")

        c = CsvConnector(engine_url="csv://" + EXAMPLE_CSV_PATH + ";encoding=latin-1")
        self.assertEqual("latin-1", c.encoding, "Can't override default encoding")

    def test_csv_engine_decode(self):

        c = CsvConnector(engine_url="csv:///data/abc.csv")
        a = c.engine_params
        expected_path = "/data/abc.csv"
        if True or os.path.sep != '/':
            expected_path = expected_path.replace('/', os.path.sep)
        self.assertEqual(expected_path, a.file_path)

        c = CsvConnector("csv:///data/abc.csv;encoding=latin-1;start=3;end=100")
        with self.assertRaises(NotImplementedError):
            c.engine_params

        a = c._engine_params
        expected_path = "/data/abc.csv"
        if True or os.path.sep != '/':
            expected_path = expected_path.replace('/', os.path.sep)
        self.assertEqual(expected_path, a.file_path)
        self.assertEqual("latin-1", a.encoding)
        self.assertEqual(3, a.start)
        self.assertEqual(100, a.end)

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

    def test_csv_missing_values(self):
        """
        Approx position in file not working when None values are in the CSV.
        """
        c = CsvConnector(engine_url="csv://" + EXAMPLE_CSV_BROKEN_PATH)
        current_position = 0
        for _ in c:
            self.assertTrue(c.progress > current_position)
            current_position = c.progress

    def test_csv_without_fieldname_header(self):

        c = CsvConnector(engine_url="csv://" + EXAMPLE_CSV_MICE,
                         field_names=['common_name', 'scientific_name', 'geo_distribution']
                         )
        mice = [mouse.as_dict() for mouse in c]

        expected_line_0 = {'common_name': 'Yellow-necked mouse',
                           'scientific_name': 'Apodemus flavicollis',
                           'geo_distribution': 'Europe'
                           }

        self.assertEqual(3, len(mice))
        # just checking first line is data with correct field names
        self.assertEqual(expected_line_0, mice[0])

    def test_csv_without_fieldname_header_write(self):
        """
        Specify fields. Without this fields are taken from first record to be added.
        """
        data_dir = tempfile.mkdtemp()
        csv_file = os.path.join(data_dir, "lemurs.csv")
        c = CsvConnector(engine_url="csv://" + csv_file, access=ayeaye.AccessMode.WRITE,
                         field_names=['common_name', 'main_colours']
                         )
        for lemur in [{'common_name': 'Indri'},
                      {'common_name': 'Ring tailed', 'main_colours': 'grey, black, white'},
                      ]:
            c.add(lemur)

        c.close_connection()

        with open(csv_file, 'r', encoding=c.encoding) as f:
            csv_content = f.read()

        expected_content = ('common_name,main_colours\n'
                            'Indri,\n'
                            'Ring tailed,"grey, black, white"\n'
                            )

        self.assertEqual(expected_content, csv_content)

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

import os
import tempfile
import unittest

import ayeaye
from ayeaye.connectors.json_connector import JsonConnector

from . import TEST_DATA_PATH

EXAMPLE_JSON_PATH = os.path.join(TEST_DATA_PATH, "london_weather.json")


class TestConnectors(unittest.TestCase):
    def test_json_basics(self):
        c = JsonConnector(engine_url="json://" + EXAMPLE_JSON_PATH)
        self.assertEqual("London", c.data.name)
        self.assertEqual("light intensity drizzle", c.data.weather.description)

    def test_json_write(self):
        data_dir = tempfile.mkdtemp()
        json_file = os.path.join(data_dir, "chips.json")
        c = JsonConnector(engine_url=f"json://{json_file}", access=ayeaye.AccessMode.WRITE)

        good_examples = [
            ("a string", '"a string"'),
            (ayeaye.Pinnate({"a": 1}), '{"a": 1}'),
            ({"a": 1}, '{"a": 1}'),
            ([1, 2, 3], "[1, 2, 3]"),
        ]

        for acceptable_data, expected_json in good_examples:
            c.data = acceptable_data
            with open(json_file, "r", encoding=c.encoding) as f:
                json_not_decoded = f.read()
                self.assertEqual(expected_json, json_not_decoded)

        # these data types can't be encoded as JSON
        bad_examples = [
            set([1, 2, 3]),
            c,
        ]
        for unacceptable_data in bad_examples:
            with self.assertRaises(TypeError):
                c.data = unacceptable_data

    def test_json_datasource_exists(self):
        c = JsonConnector(engine_url="json://" + EXAMPLE_JSON_PATH)
        self.assertTrue(c.datasource_exists)

        c = JsonConnector(engine_url="json://this_doesnt_exist.json")
        self.assertFalse(c.datasource_exists)

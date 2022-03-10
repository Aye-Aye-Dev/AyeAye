import os
import tarfile
import tempfile
import unittest

import ayeaye

# from ayeaye.connectors.flowerpot import FlowerpotEngine, FlowerPotConnector
# from ayeaye.connectors.json_connector import JsonConnector
from ayeaye.connectors.multi_connector import MultiConnector

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
# EXAMPLE_FLOWERPOT_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'exampleflowerpot.tar.gz')
# EXAMPLE_ENGINE_URL = 'gs+flowerpot://fake_flowerpot_bucket/some_file.json'
# EXAMPLE_JSON_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'london_weather.json')
EXAMPLE_CSV_MICE = os.path.join(PROJECT_TEST_PATH, "data", "mice.csv")
EXAMPLE_CSV_SQUIRRELS = os.path.join(PROJECT_TEST_PATH, "data", "squirrels.csv")


class TestMultiConnectors(unittest.TestCase):
    def test_multi_connector_append(self):
        """
        Add engine_urls at runtime.
        This is a preemptive measure for when the post build lock file (not yet implemented) will
        store runtime changes to the data connections.
        """
        c = MultiConnector(
            engine_url=["csv://data_1234.csv", "csv://data_4567.csv"],
            access=ayeaye.AccessMode.WRITE,
        )
        c.engine_url.append("csv://data_8910.csv")

        all_urls = [connector.engine_url for connector in c]
        expected_urls = ["csv://data_1234.csv", "csv://data_4567.csv", "csv://data_8910.csv"]
        self.assertEqual(expected_urls, all_urls)

        # check that late additions to c.engine_url are visible. 'late' means after c.connect()
        # has been called
        another_file = "csv://data_1112.csv"
        c.engine_url.append(another_file)
        all_urls = [connector.engine_url for connector in c]
        expected_urls.append(another_file)
        self.assertEqual(expected_urls, all_urls)

    def test_multi_connector_passes_args(self):
        """
        kwargs given to Connect should be passed to each DataConnection created by multi-connect.
        """
        # header-less CSVs
        c = MultiConnector(
            engine_url=["csv://" + EXAMPLE_CSV_MICE, "csv://" + EXAMPLE_CSV_SQUIRRELS],
            access=ayeaye.AccessMode.READ,
            field_names=["common_name", "scientific_name", "geo_distribution"],
        )
        rodents = []
        for cx in c:
            rodents.extend([r.as_dict() for r in cx])

        expected_line_0 = {
            "common_name": "Yellow-necked mouse",
            "scientific_name": "Apodemus flavicollis",
            "geo_distribution": "Europe",
        }

        expected_line_3 = {
            "common_name": "American red squirrel",
            "scientific_name": "Tamiasciurus hudsonicus",
            "geo_distribution": "North America",
        }

        self.assertEqual(5, len(rodents))
        self.assertEqual(expected_line_0, rodents[0])
        self.assertEqual(expected_line_3, rodents[3])

    def test_multi_connector_by_engine_url(self):
        """
        Get a data connection from a multi connector by engine_url
        """
        engine_0 = "csv://" + EXAMPLE_CSV_MICE
        engine_1 = "csv://" + EXAMPLE_CSV_SQUIRRELS
        c = MultiConnector(
            engine_url=[engine_0, engine_1],
            access=ayeaye.AccessMode.READ,
            field_names=["common_name", "scientific_name", "geo_distribution"],
        )

        dataset = c[engine_0]
        # check access to any dataset property
        self.assertTrue(dataset.engine_params.file_path.endswith("tests/data/mice.csv"))
        self.assertEqual(2, len(c))

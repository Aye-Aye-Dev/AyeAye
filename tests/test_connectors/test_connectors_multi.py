import os
import unittest

import ayeaye

from ayeaye.connectors.multi_connector import MultiConnector

from . import TEST_DATA_PATH

EXAMPLE_CSV_MICE = os.path.join(TEST_DATA_PATH, "mice.csv")
EXAMPLE_CSV_SQUIRRELS = os.path.join(TEST_DATA_PATH, "squirrels.csv")


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
        self.assertTrue(dataset.engine_params.file_path.endswith("data/mice.csv"))
        self.assertEqual(2, len(c))

    def test_wildcards(self):
        """
        When * or ? are used in engine_url a filesystem search should result in a MultiConnector.
        See related test :method:`TestResolve.test_wildcard_with_resolver`
        """
        search_path = os.path.join(TEST_DATA_PATH, "m*.?sv")
        msg = "expected mice.csv and monkeys.tsv"

        wildcard_connector = ayeaye.Connect(engine_url="file://" + search_path)

        resolved_connectors = []
        for connector in wildcard_connector:
            resolved_connectors.append(connector.engine_params.file_path)

        self.assertEqual(2, len(resolved_connectors), msg)
        files = [file_path.split(os.path.sep)[-1] for file_path in resolved_connectors]
        self.assertIn("mice.csv", files, msg)
        self.assertIn("monkeys.tsv", files, msg)

    def test_multi_connector_duplicate_engine_url(self):
        """
        Get a data connection from a multi connector by engine_url
        """
        engine_url = "csv://" + EXAMPLE_CSV_MICE
        c = MultiConnector(engine_url=[], access=ayeaye.AccessMode.READ)

        connector_0 = c.add_engine_url(engine_url)
        connector_1 = c.add_engine_url(engine_url)

        self.assertTrue(engine_url == connector_0.engine_url == connector_1.engine_url)

    def test_multi_connector_duplicate_engine_url_with_resolver(self):
        """
        Multiple engine_urls resolve into the same thing so should return a single connector.
        """
        engine_url = "csv://" + EXAMPLE_CSV_MICE.replace("mice.csv", "{some_file}")

        c = MultiConnector(engine_url=[], access=ayeaye.AccessMode.READ)

        with ayeaye.connector_resolver.context(some_file="mice.csv"):
            connector_0 = c.add_engine_url(engine_url)
            connector_1 = c.add_engine_url(engine_url)

            self.assertEqual(connector_0.engine_url, connector_1.engine_url)
            self.assertTrue(id(connector_0) == id(connector_1))

    def test_unresolved_engine_url(self):
        # replace 'mice.csv' with a template param
        engine_url = "csv://" + EXAMPLE_CSV_MICE.replace("mice.csv", "{some_file}")

        c = MultiConnector(engine_url=[], access=ayeaye.AccessMode.READ)
        connector_0 = c.add_engine_url(engine_url)
        with self.assertRaises(ValueError) as exception_context:
            _engine_url = connector_0.engine_url

        msg = "Without any context it should raise an exception"
        self.assertIn("Couldn't fully resolve engine URL", str(exception_context.exception), msg)

        c = MultiConnector(engine_url=[], access=ayeaye.AccessMode.READ)
        with ayeaye.connector_resolver.context(some_file="mice.csv"):
            connector_1 = c.add_engine_url(engine_url)
            self.assertTrue(connector_1.engine_url.endswith("mice.csv"))

"""
Created on 17 Dec 2020

@author: si
"""
import os
import tempfile
import unittest

import ayeaye
from ayeaye.connectors.ndjson_connector import NdjsonConnector

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_NDJSON_UK_PUBS = os.path.join(PROJECT_TEST_PATH, "data", "uk_pubs.ndjson")


class TestNdjsonConnector(unittest.TestCase):
    def test_iterate_over_json_lines(self):
        c = NdjsonConnector(engine_url="ndjson://" + EXAMPLE_NDJSON_UK_PUBS)
        uk_pubs_names = [pub.name for pub in c]
        expected = "The New Flying Horse"  # just check one expected value has been found
        self.assertIn(expected, uk_pubs_names)

    def test_ndjson_write(self):
        """
        Write to a file without using a schema.
        """
        data_dir = tempfile.mkdtemp()
        ndjson_file = os.path.join(data_dir, "frog_fish.ndjson")
        c = NdjsonConnector(engine_url="ndjson://" + ndjson_file, access=ayeaye.AccessMode.WRITE)

        for common_name in ["Warty frogfish", "Hairy Frogfish"]:
            p = ayeaye.Pinnate({"common_name": common_name})
            c.add(p)

        c.close_connection()  # flush to disk

        with open(ndjson_file, "r", encoding=c.encoding) as f:
            file_content = f.read()

        expected_content = '{"common_name": "Warty frogfish"}\n' '{"common_name": "Hairy Frogfish"}\n'
        self.assertEqual(expected_content, file_content)

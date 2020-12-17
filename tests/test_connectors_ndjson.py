'''
Created on 17 Dec 2020

@author: si
'''
import os
import unittest

from ayeaye.connectors.ndjson_connector import NdjsonConnector

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_NDJSON_UK_PUBS = os.path.join(PROJECT_TEST_PATH, 'data', 'uk_pubs.ndjson')


class TestNdjsonConnector(unittest.TestCase):

    def test_iterate_over_json_lines(self):
        c = NdjsonConnector(engine_url="ndjson://" + EXAMPLE_NDJSON_UK_PUBS)
        uk_pubs_names = [pub.name for pub in c]
        expected = "The New Flying Horse"  # just check one expected value has been found
        self.assertIn(expected, uk_pubs_names)

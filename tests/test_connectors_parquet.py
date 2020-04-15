'''
Created on 4 Mar 2020

@author: si
'''
import os
import unittest

import pandas as pd

from ayeaye.connectors.parquet_connector import ParquetConnector

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_HELLO = os.path.join(PROJECT_TEST_PATH, 'data', 'hello.parquet')

"""
Test data built with this-

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df = pd.DataFrame({'name': ['Alice', 'Bob'],
                   'favorite_colour': ['blue', 'green'],
                  },
                  index=list('ab'))

pq.write_table(table, 'hello.parquet')
"""


class TestSqlAlchemyConnector(unittest.TestCase):

    def test_read_as_rows(self):
        """
        Iterate through a parqet file row by row for all columns.
        """
        c = ParquetConnector(engine_url="parquet://" + EXAMPLE_HELLO)
        all_records = [r.as_dict() for r in c]

        self.assertEqual(2, len(all_records), "There are two sample records")

        # there are a couple of other keys as a result of the pandas index. Just check the
        # payload fields
        wanted = [{'name': 'Alice', 'favorite_colour': 'blue'},
                  {'name': 'Bob', 'favorite_colour': 'green'}
                  ]
        for idx, expected_row in enumerate(wanted):
            for expected_key, expected_value in expected_row.items():
                self.assertEqual(expected_value, all_records[idx][expected_key])

    def test_read_as_pandas(self):
        c = ParquetConnector(engine_url="parquet://" + EXAMPLE_HELLO)
        p = c.as_pandas()

        self.assertIsInstance(p, pd.DataFrame)
        self.assertEqual('Alice', p['name'][0], "Can't find expected value in Pandas dataframe")

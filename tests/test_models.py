import os
import unittest

import ayeaye

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_CSV_PATH = os.path.join(PROJECT_TEST_PATH, 'data', 'deadly_creatures.csv')


class FakeModel(ayeaye.Model):
    animals = ayeaye.Connect(engine_url=f"csv://{EXAMPLE_CSV_PATH}")

    def build(self):
        for a in self.animals:
            self.log(a.common_name)


class TestModels(unittest.TestCase):

    def test_go_closes_dataset_connections(self):

        m = FakeModel()
        m.log_to_stdout = False
        m.go()

        self.assertEqual(None, m.animals.file_handle, "File handle should be closed")

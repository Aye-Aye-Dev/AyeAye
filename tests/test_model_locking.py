'''
Created on 16 Jun 2021

@author: si
'''
import os
import unittest

import ayeaye

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_PATH = os.path.join(PROJECT_TEST_PATH, 'data')


class FakeModel(ayeaye.Model):
    animals = ayeaye.Connect(engine_url=f"csv://{TEST_DATA_PATH}/" + "{animal_type}.csv",
                             )

    def build(self):
        for a in self.animals:
            self.log(a.common_name)


class TestModelLocking(unittest.TestCase):

    def test_simple_mapping(self):
        """
        Dictionary mapping passed to .context should be in the lock document.
        """
        # data_version
        with ayeaye.connector_resolver.context(animal_type='deadly_creatures'):
            m = FakeModel()
            lock_doc = m.lock()

        self.assertEqual('deadly_creatures', lock_doc['resolve_context']['mapper']['animal_type'])

    def test_lock_all_datasets(self):
        """
        With LockingMode.ALL_DATASETS , engine URLS from all datasets are captured
        """
        with ayeaye.connector_resolver.context(animal_type='deadly_creatures'):
            m = FakeModel()
            lock_doc = m.lock(locking_level=ayeaye.LockingMode.ALL_DATASETS)

        expected_file_ending = 'tests/data/deadly_creatures.csv'
        self.assertTrue(lock_doc['dataset_engine_urls']['animals'].endswith(expected_file_ending))

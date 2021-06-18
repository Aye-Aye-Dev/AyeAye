import json
import os
import shutil
import tempfile
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

    def setUp(self):
        self._working_directory = None

    def tearDown(self):
        if self._working_directory and os.path.isdir(self._working_directory):
            shutil.rmtree(self._working_directory)

    def working_directory(self):
        self._working_directory = tempfile.mkdtemp()
        return self._working_directory

    def test_go_closes_dataset_connections(self):

        m = FakeModel()
        m.log_to_stdout = False
        m.go()

        self.assertEqual(None, m.animals.file_handle, "File handle should be closed")

    def test_double_usage(self):
        """
        Use Connect as a callable to make a second iterable. Make the cartesian product to
        demonstrate they are independent.
        """
        class AnimalsModel(ayeaye.Model):
            animals_a = ayeaye.Connect(engine_url="csv://" + EXAMPLE_CSV_PATH)
            animals_b = animals_a.clone()
            animals_output = ayeaye.Connect(access=ayeaye.AccessMode.WRITE)

            def build(self):
                cartesian = []
                for a in self.animals_a:
                    for b in self.animals_b:
                        cartesian.append(f"{a.common_name}_{b.common_name}")
                self.animals_output.data = cartesian

        self.assertNotEqual(id(AnimalsModel.animals_a), id(AnimalsModel.animals_b))

        m = AnimalsModel()

        output_file = "{}/animals_summary.json".format(self.working_directory())
        m.animals_output.update(engine_url=f"json://{output_file};indent=4")
        output_encoding = m.animals_output.encoding

        m.go()

        with open(output_file, 'r', encoding=output_encoding) as f:
            output_data = json.load(f)

        expected_data = ['Crown of thorns starfish_Crown of thorns starfish',
                         'Crown of thorns starfish_Golden dart frog'
                         ]
        self.assertEqual(expected_data, output_data)

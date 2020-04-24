from io import StringIO
import json
import os
import shutil
import tempfile
import unittest

from examples.favourite_colours import FavouriteColours
from examples.poisonous_animals import PoisonousAnimals

EXAMPLE_MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'examples')


class TestExamples(unittest.TestCase):

    def setUp(self):
        self._working_directory = None

        # the example models use relative paths to the example data
        self.current_working_directory = os.getcwd()
        os.chdir(EXAMPLE_MODELS_DIR)

    def tearDown(self):
        if self._working_directory and os.path.isdir(self._working_directory):
            shutil.rmtree(self._working_directory)

        os.chdir(self.current_working_directory)

    def working_directory(self):
        self._working_directory = tempfile.mkdtemp()
        return self._working_directory

    def test_poisonous_animals_fetch_data(self):
        """
        This model reads a local .flowerpot file. Run it and check the logged output (as it doesn't
        have any output datasets).
        """
        # relative path is used in connection
        external_log = StringIO()

        m = PoisonousAnimals()
        m.set_logger(external_log)
        m.log_to_stdout = False
        m.go()

        external_log.seek(0)
        all_the_logs = external_log.read()
        expected = 'In Australia you could find Blue ringed octopus,Box jellyfish,Eastern brown snake'
        assert expected in all_the_logs

    def test_favourite_colours_build_sample_data(self):
        """
        Using .build() and not .go() so pre and post checks aren't tested.
        """
        output_file = "{}/favourite_colours_summary.json".format(self.working_directory())
        m = FavouriteColours()
        # give the connector a new output file
        m.favourites_summary(engine_url=f"json://{output_file};indent=4")
        m.build()
        output_encoding = m.favourites_summary.encoding
        m.close_datasets()

        with open(output_file, 'r', encoding=output_encoding) as f:
            output_data = json.load(f)

        # check Blue which has dates 2020-01-01 - 2020-02-15
        self.assertEqual(31, output_data["Blue"]["January"])
        self.assertEqual(14, output_data["Blue"]["February"])

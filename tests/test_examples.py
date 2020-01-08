from io import StringIO
import os
import unittest

from examples.poisonous_animals import PoisonousAnimals

EXAMPLE_MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'examples')


class TestExamples(unittest.TestCase):

    def test_poisonous_animals_fetch_data(self):
        """
        This model reads a local .flowerpot file. Run it and check the logged output (as it doesn't
        have any output datasets).
        """
        # relative path is used in connection
        current_working_directory = os.getcwd()
        os.chdir(EXAMPLE_MODELS_DIR)
        external_log = StringIO()
    
        m = PoisonousAnimals()
        m.set_logger(external_log)
        m.log_to_stdout = False
        m.go()
    
        external_log.seek(0)
        all_the_logs = external_log.read()
        expected = 'In Australia you could find Blue ringed octopus,Box jellyfish,Eastern brown snake'
        assert expected in all_the_logs
    
        os.chdir(current_working_directory)

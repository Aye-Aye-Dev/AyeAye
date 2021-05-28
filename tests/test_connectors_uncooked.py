from io import StringIO
import os
import shutil
import tempfile
import unittest

import ayeaye
from ayeaye.connectors import UncookedConnector

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_FILE = os.path.join(PROJECT_TEST_PATH, 'data', 'quote.txt')


class FakeModel(ayeaye.Model):
    quote = ayeaye.Connect(engine_url=f"file://{EXAMPLE_FILE}")

    def build(self):
        self.log(self.quote.data)


class TestUncookedConnector(unittest.TestCase):

    def setUp(self):
        self._working_directory = None

    def tearDown(self):
        if self._working_directory and os.path.isdir(self._working_directory):
            shutil.rmtree(self._working_directory)

    def working_directory(self):
        self._working_directory = tempfile.mkdtemp()
        return self._working_directory

    def test_read_access(self):
        """Model uses generic connection (file://) to read contents of a file which is written to
        the log.
        """
        m = FakeModel()
        m.log_to_stdout = False
        external_log = StringIO()
        m.set_logger(external_log)
        m.go()

        external_log.seek(0)
        all_the_logs = external_log.read()
        self.assertIn("Perfection is achieved", all_the_logs)

    def test_on_demand(self):
        "File handle and file path are available as attributes"
        c = UncookedConnector(engine_url=f"file://{EXAMPLE_FILE}")

        self.assertEqual(EXAMPLE_FILE, c.file_path)

        fh = c.file_handle
        self.assertIsNotNone(fh, "Should be a file handle")

        c.close_connection()

        self.assertNotEqual(fh, c.file_handle, "A new file handle should have been opened")

    def test_write(self):

        a_quote = ("More data does not necessarily mean better information."
                   "Bruce Schnier - Cryptogram Oct. 2013"
                   )

        data_dir = tempfile.mkdtemp()
        quotes_file = os.path.join(data_dir, "quotes.notes")

        c = UncookedConnector(engine_url="file://" + quotes_file, access=ayeaye.AccessMode.WRITE)
        c.data = a_quote
        c.close_connection()

        with open(quotes_file, 'r', encoding=c.encoding) as f:
            file_content = f.read()

        self.assertEqual(a_quote, file_content)

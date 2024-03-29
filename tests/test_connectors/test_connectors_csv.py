from datetime import datetime, timezone
import os
import tempfile
import unittest

import ayeaye
from ayeaye.connectors.csv_connector import CsvConnector, TsvConnector

from . import TEST_DATA_PATH

# PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_CSV_PATH = os.path.join(TEST_DATA_PATH, "deadly_creatures.csv")
EXAMPLE_TSV_PATH = os.path.join(TEST_DATA_PATH, "monkeys.tsv")
EXAMPLE_CSV_VENOMOUS = os.path.join(TEST_DATA_PATH, "venomous_creatures.csv")
EXAMPLE_CSV_MICE = os.path.join(TEST_DATA_PATH, "mice.csv")
EXAMPLE_CSV_SQUIRRELS = os.path.join(TEST_DATA_PATH, "squirrels.csv")
EXAMPLE_CSV_DUPLICATE_FIELDNAMES = os.path.join(TEST_DATA_PATH, "duplicate_field_names.csv")
EXAMPLE_CSV_QUOTED = os.path.join(TEST_DATA_PATH, "venomous_spiders.csv")


class TestConnectorsCsv(unittest.TestCase):
    def test_csv_basics(self):
        """
        Iterate all the data items and check each row is being yielded as an instance of
        :class:`ayeaye.Pinnate`
        """
        c = CsvConnector(engine_url="csv://" + EXAMPLE_CSV_PATH)

        animals_names = ", ".join([deadly_animal.common_name for deadly_animal in c])
        expected = "Crown of thorns starfish, Golden dart frog"
        assert expected == animals_names

    def test_csv_write(self):
        """
        Write to a CSV without using a schema.
        """
        data_dir = tempfile.mkdtemp()
        csv_file = os.path.join(data_dir, "fish.csv")
        c = CsvConnector(engine_url="csv://" + csv_file, access=ayeaye.AccessMode.WRITE)

        # two data types that can be added
        p = ayeaye.Pinnate({"common_name": "Angel fish"})
        c.add(p)

        d = {"common_name": "Grey reef shark"}
        c.add(d)
        c.close_connection()  # flush to disk

        with open(csv_file, "r", encoding=c.encoding) as f:
            csv_content = f.read()

        expected_content = "common_name\n" "Angel fish\n" "Grey reef shark\n"

        self.assertEqual(expected_content, csv_content)

    def test_tsv_basics(self):
        """
        Tab separated, Iterate all the data items and check each row is being yielded as an
        instance of :class:`ayeaye.Pinnate`
        """
        c = TsvConnector(engine_url="tsv://" + EXAMPLE_TSV_PATH)

        monkey_names = ", ".join([monkey.common_name for monkey in c])
        expected = "Goeldi's marmoset, Common squirrel monkey, Crab-eating macaque"
        assert expected == monkey_names

    def test_csv_encoding(self):
        """
        Specify character encoding in the URL. This test doesn't ensure data conforms.
        """
        c = CsvConnector(engine_url="csv://" + EXAMPLE_CSV_PATH)
        self.assertEqual("utf-8-sig", c.encoding, "Unexpected default encoding")

        c = CsvConnector(engine_url="csv://" + EXAMPLE_CSV_PATH + ";encoding=latin-1")
        self.assertEqual("latin-1", c.encoding, "Can't override default encoding")

    def test_csv_engine_decode(self):
        c = CsvConnector(engine_url="csv:///data/abc.csv")
        a = c.engine_params
        expected_path = "/data/abc.csv"
        if True or os.path.sep != "/":
            expected_path = expected_path.replace("/", os.path.sep)
        self.assertEqual(expected_path, a.file_path)

        c = CsvConnector("csv:///data/abc.csv;encoding=latin-1;start=3;end=100")
        with self.assertRaises(NotImplementedError):
            c.engine_params

        a = c._engine_params
        expected_path = "/data/abc.csv"
        if True or os.path.sep != "/":
            expected_path = expected_path.replace("/", os.path.sep)
        self.assertEqual(expected_path, a.file_path)
        self.assertEqual("latin-1", a.encoding)
        self.assertEqual(3, a.start)
        self.assertEqual(100, a.end)

    def test_csv_missing_values(self):
        """
        Approx position in file not working when None values are in the CSV.
        """
        c = CsvConnector(engine_url="csv://" + EXAMPLE_CSV_VENOMOUS)
        current_position = 0
        for _ in c:
            self.assertTrue(c.progress > current_position)
            current_position = c.progress

    def test_csv_without_fieldname_header(self):
        c = CsvConnector(
            engine_url="csv://" + EXAMPLE_CSV_MICE,
            field_names=["common_name", "scientific_name", "geo_distribution"],
        )
        mice = [mouse.as_dict() for mouse in c]

        expected_line_0 = {
            "common_name": "Yellow-necked mouse",
            "scientific_name": "Apodemus flavicollis",
            "geo_distribution": "Europe",
        }

        self.assertEqual(3, len(mice))
        # just checking first line is data with correct field names
        self.assertEqual(expected_line_0, mice[0])

    def test_csv_without_fieldname_header_write(self):
        """
        Specify fields. Without this fields are taken from first record to be added.
        """
        data_dir = tempfile.mkdtemp()
        csv_file = os.path.join(data_dir, "lemurs.csv")
        c = CsvConnector(
            engine_url="csv://" + csv_file,
            access=ayeaye.AccessMode.WRITE,
            field_names=["common_name", "main_colours"],
        )
        for lemur in [
            {"common_name": "Indri"},
            {"common_name": "Ring tailed", "main_colours": "grey, black, white"},
        ]:
            c.add(lemur)

        c.close_connection()

        with open(csv_file, "r", encoding=c.encoding) as f:
            csv_content = f.read()

        expected_content = (
            "common_name,main_colours\n" "Indri,\n" 'Ring tailed,"grey, black, white"\n'
        )

        self.assertEqual(expected_content, csv_content)

    def test_csv_extra_fields_with_write(self):
        """
        When the optional 'field_names' argument is used when writing to a CSV any additional
        fields in the data passed to :meth:`add` will be ignored.
        """
        data_dir = tempfile.mkdtemp()
        csv_file = os.path.join(data_dir, "bird_names.csv")
        c = CsvConnector(
            engine_url="csv://" + csv_file,
            access=ayeaye.AccessMode.WRITE,
            field_names=["common_name"],
        )
        for bird in [
            {"common_name": "Bull finch", "habitat": "Orchards"},
            {"common_name": "Ringed Plover", "main_colours": "grey, black, white"},
        ]:
            c.add(bird)

        c.close_connection()

        with open(csv_file, "r", encoding=c.encoding) as f:
            csv_content = f.read()

        expected_content = "common_name\nBull finch\nRinged Plover\n"
        self.assertEqual(expected_content, csv_content)

    def test_required_fields(self):
        c = CsvConnector(
            engine_url="csv://" + EXAMPLE_CSV_PATH,
            required_fields=["common_name"],
        )
        r = next(iter(c))
        # doesn't raise an exception as given required field is present
        self.assertIsInstance(r, ayeaye.Pinnate)

        c = CsvConnector(
            engine_url="csv://" + EXAMPLE_CSV_PATH,
            required_fields=["common_name", "native_to", "unknown_field"],
        )
        # missing required field
        with self.assertRaises(ValueError):
            next(iter(c))

    def test_expected_fields(self):
        c = CsvConnector(
            engine_url="csv://" + EXAMPLE_CSV_PATH,
            expected_fields=["common_name", "native_to"],
        )
        r = next(iter(c))
        # doesn't raise an exception as fields are exactly as given in file's header
        self.assertIsInstance(r, ayeaye.Pinnate)

        c = CsvConnector(
            engine_url="csv://" + EXAMPLE_CSV_PATH,
            expected_fields=["common_name"],
        )
        # missing field
        with self.assertRaises(ValueError):
            next(iter(c))

    def test_alias_fields_dictionary(self):
        c = CsvConnector(
            engine_url="csv://" + EXAMPLE_CSV_PATH,
            alias_fields={"common_name": "animal_name"},
        )
        r = next(iter(c))
        self.assertEqual("Crown of thorns starfish", r.animal_name)

    def test_alias_fields_complete_replace(self):
        c = CsvConnector(
            engine_url="csv://" + EXAMPLE_CSV_PATH,
            alias_fields=["animal_name", "lives"],
        )
        actual = next(iter(c))
        expected = {"animal_name": "Crown of thorns starfish", "lives": "Indo-Pacific"}
        self.assertEqual(expected, actual.as_dict())

    def test_duplicate_fieldnames(self):
        """Simple way to handle CSV file with non-unique field names."""

        c = CsvConnector(
            engine_url="csv://" + EXAMPLE_CSV_DUPLICATE_FIELDNAMES,
            expected_fields=[
                "Species",
                "Habitat",
                "Description",
                "Oviparity-viviparity",
                "Description",
            ],
            alias_fields=[
                "species",
                "habitat_type",
                "habitat_description",
                "ovi-vivi",
                "ov_description",
            ],
        )
        actual = next(iter(c))

        expected = {
            "species": "Tiger shark",
            "habitat_type": "Tropical",
            "habitat_description": (
                "Populations are found in many tropical and temperate"
                " waters, especially around central Pacific islands."
            ),
            "ovi-vivi": "Ovoviviparous",
            "ov_description": (
                "Eggs hatch internally and the young are born live when " "fully developed"
            ),
        }
        self.assertEqual(expected, actual.as_dict())

    def test_incompatible_optional_args(self):
        c = CsvConnector(
            engine_url="csv://" + EXAMPLE_CSV_MICE,
            field_names=["common_name", "scientific_name", "geo_distribution"],
            alias_fields=["a", "b", "c"],
        )

        # Can't have both field_names and alias_fields, just use alias field!
        with self.assertRaises(ValueError):
            next(iter(c))

    def test_optional_args_with_write(self):
        """Read only connector arguments raise exception when called in write mode."""
        data_dir = tempfile.mkdtemp()
        csv_file = os.path.join(data_dir, "garden_insects.csv")

        for optional_field in ["required_fields", "expected_fields", "alias_fields"]:
            c = CsvConnector(
                engine_url="csv://" + csv_file,
                access=ayeaye.AccessMode.WRITE,
                **{optional_field: "xyz"},
            )
            with self.assertRaises(ValueError):
                c.add({"common_name": "Grasshopper"})

    def test_last_modified(self):
        "specific to `FileBasedConnector`"
        data_dir = tempfile.mkdtemp()
        csv_file = os.path.join(data_dir, "garden_insects.csv")

        c = CsvConnector(
            engine_url="csv://" + csv_file,
        )

        # file doesn't exist yet
        self.assertEqual(c.last_modified, None)

        f = open(csv_file, "w")
        f.write("1, 2, 3")
        f.close()

        self.assertIsInstance(c.last_modified, datetime)
        self.assertEqual(c.last_modified.tzinfo, timezone.utc)

    def test_quoting_read(self):
        """
        Quoting mode when reading - toxic_scale field is a number. The CSV module makes this into
        a float.
        """
        c = CsvConnector(engine_url="csv://" + EXAMPLE_CSV_QUOTED, quoting="QUOTE_NONNUMERIC")
        first_row = next(iter(c))
        self.assertIsInstance(first_row.toxic_scale, float)

    def test_quoting_write(self):
        data_dir = tempfile.mkdtemp()
        csv_file = os.path.join(data_dir, "bird_tally.csv")
        c = CsvConnector(
            engine_url="csv://" + csv_file,
            access=ayeaye.AccessMode.WRITE,
            field_names=["common_name", "number_seen"],
            quoting="QUOTE_NONNUMERIC",
        )
        for bird in [
            {"common_name": "Red kite", "number_seen": 101},
        ]:
            c.add(bird)

        c.close_connection()

        with open(csv_file, "r", encoding=c.encoding) as f:
            csv_content = f.read()

        expected_content = '"common_name","number_seen"\n"Red kite",101\n'
        self.assertEqual(expected_content, csv_content)

    def test_transforms(self):
        """
        Transform a datetime into a string so it can be written to a CSV and then
        back again.
        """

        def date_to_string(dt):
            if dt is None:
                return None
            return dt.isoformat()

        def date_from_string(dtstr):
            if dtstr is None or dtstr == "":
                return None
            return datetime.fromisoformat(dtstr)

        data_dir = tempfile.mkdtemp()
        csv_file = os.path.join(data_dir, "bird_spotting.csv")

        c = CsvConnector(
            engine_url="csv://" + csv_file,
            access=ayeaye.AccessMode.WRITE,
            field_names=["common_name", "when_spotted"],
            transform_map={"when_spotted": date_to_string},
        )
        for bird in [
            {"common_name": "Lammergeier", "when_spotted": datetime(2023, 7, 26)},
        ]:
            c.add(bird)

        c.close_connection()

        c = CsvConnector(
            engine_url="csv://" + csv_file, transform_map={"when_spotted": date_from_string}
        )
        first_row = next(iter(c))
        self.assertIsInstance(first_row.when_spotted, datetime)
        self.assertEqual(first_row.when_spotted, datetime(2023, 7, 26))

from datetime import datetime
import os
import unittest

from ayeaye.connect import Connect
from ayeaye.connectors.engine_type_modifiers import engine_type_modifier_factory
from ayeaye.connectors.engine_type_modifiers.smart_open_modifier import SmartOpenModifier
from ayeaye.connectors.engine_type_modifiers.utils import s3_pattern_match
from ayeaye.connectors.ndjson_connector import NdjsonConnector


from . import TEST_DATA_PATH

EXAMPLE_COMPRESSED_PATH = os.path.join(TEST_DATA_PATH, "national_parks.ndjson.gz")


class TestEngineTypeModifier(unittest.TestCase):
    def test_engine_type_modifier_factory(self):
        ModifierCls = engine_type_modifier_factory(
            connector_cls=NdjsonConnector,
            modifier_labels=["gz", "s3"],
        )

        msg = "SmartOpenModifier supports this combination."
        self.assertEqual(SmartOpenModifier, ModifierCls, msg)

    def test_dynamic_composition(self):
        "Create a DataConnector 'like' class"

        DynamicDataConnector = SmartOpenModifier.apply(
            connector_cls=NdjsonConnector,
            modifier_labels=["gz", "s3"],
        )

        self.assertEqual(DynamicDataConnector.requested_modifier_labels, ["gz", "s3"])

    def test_file_gz(self):
        """
        The 'gz+' prefix should result in transparent decompression of a filesystem file by using
        smart open.
        """
        c = Connect(engine_url="gz+ndjson://" + EXAMPLE_COMPRESSED_PATH)
        first_record = next(iter(c))
        self.assertEqual("NEW FOREST", first_record.name, "Known first record in sample data")

    def test_filesystem_listing(self):
        """
        When 'gz+' is used as the modifier it should be possible to use wildcards to expand to
        a list of MultiConnectors which each transparently decompress and use ndjson.
        """
        filesystem_pattern = EXAMPLE_COMPRESSED_PATH.replace("national_parks.ndjson.gz", "*.gz")
        self.assertTrue(filesystem_pattern.endswith("*.gz"), "test file has changed")

        c = Connect(engine_url=f"gz+ndjson://{filesystem_pattern}")
        msg = "There should be one connector to represent the only compressed sample file"
        self.assertEqual(1, len(c.data), msg)

        # first record in first (i.e. only) connector
        first_record = next(iter(c.data[0]))
        self.assertEqual("NEW FOREST", first_record.name, "Known first record in sample data")

    def test_s3_pattern_match(self):
        prefix, matcher = s3_pattern_match("my_directory/sub_directory/*.csv")
        self.assertEqual("my_directory/sub_directory/", prefix)
        self.assertTrue(matcher("my_directory/sub_directory/hello.csv"))
        self.assertFalse(matcher("my_directory/sub_directory/hello.csv/ooops"))
        self.assertFalse(matcher("my_directory/sub_directory/hello csv"))

        for unsupported_pattern in [
            "data/\./*.csv",
            "data/?.csv/*",
        ]:
            with self.assertRaises(NotImplementedError):
                s3_pattern_match(unsupported_pattern)

    def test_last_modified(self):
        """
        SmartOpenModifier should override base method. Actual override not checked, just coverage.
        """
        c = Connect(engine_url="gz+ndjson://" + EXAMPLE_COMPRESSED_PATH)
        self.assertIsInstance(c.last_modified, datetime)

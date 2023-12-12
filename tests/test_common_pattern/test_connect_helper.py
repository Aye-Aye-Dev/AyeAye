import tempfile
import os
import unittest

from ayeaye.common_pattern.connect_helper import MultiConnectorNewDataset
from ayeaye.connect import Connect
from ayeaye.connect_resolve import connector_resolver
from ayeaye.connectors.base import AccessMode


class TestConnectHelper(unittest.TestCase):
    def test_engine_from_manifest(self):
        data_dir = tempfile.mkdtemp()

        # the product_ template variables are resolved by .new_dataset below
        output_template = "csv://{datasets}/{product_group}_{product_name}_parts_list.csv"
        components_doc = Connect(
            engine_url=[],
            method_overlay=(MultiConnectorNewDataset(template=output_template), "new_dataset"),
            access=AccessMode.WRITE,
        )

        # the data output directory is resolved by the normal Aye-aye connector_resolver
        with connector_resolver.context(datasets=data_dir):
            components = components_doc.new_dataset(
                product_group="machinery",
                product_name="digger",
            )

            # components is now a new CSV data connector, add something to it
            components.add({"name": "spring", "product_code": "ab123"})
            components.close_connection()

        expected_file = os.path.join(data_dir, "machinery_digger_parts_list.csv")
        self.assertTrue(os.path.exists(expected_file))

        with open(expected_file, encoding="utf-8-sig") as f:
            file_contents = f.read()

        expected_contents = "name,product_code\nspring,ab123\n"
        self.assertEqual(expected_contents, file_contents)

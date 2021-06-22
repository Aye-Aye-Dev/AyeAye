import os
import unittest

from ayeaye.common_pattern.manifest import EngineFromManifest
from ayeaye.connect_resolve import connector_resolver
from ayeaye.connect import Connect
from ayeaye.model import Model

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
TEST_DATA = os.path.join(PROJECT_TEST_PATH, "..", "data")


class TestManifest(unittest.TestCase):

    def test_engine_from_manifest(self):
        """
        Use list of files from manifest to load other datasets.

        @see notes in EngineFromManifest

        ./data/manifest_abcd.json contains a list of files, well it could be a list but is just
        one file - 'blue_ants.csv'.

        'abcd' is the build serial number.
        """
        class InsectSurvey(Model):
            manifest = Connect(engine_url=f"json://{TEST_DATA}/manifest_" + "{build_id}.json")
            ants = Connect(engine_url=EngineFromManifest(manifest, "source_files", "csv"))
            invertebrates = Connect(engine_url=EngineFromManifest(manifest, "single_file", "json"))

            def build(self):
                return

        with connector_resolver.context(build_id="abcd"):
            m = InsectSurvey()
            m.go()  # uses pre_build(), build() etc.
            ants_engine_url = m.ants.engine_url
            invertebrates_engine_url = m.invertebrates.engine_url

        self.assertEqual(ants_engine_url, ["csv://blue_ants.csv"])
        self.assertEqual(invertebrates_engine_url, "json://worms.json")

import os
import unittest

from ayeaye.common_pattern.manifest import AbstractManifestMapper, EngineFromManifest
from ayeaye.connect_resolve import connector_resolver
from ayeaye.connect import Connect
from ayeaye.model import Model

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
TEST_DATA = os.path.join(PROJECT_TEST_PATH, "..", "data")


class MagicMapper(AbstractManifestMapper):
    def map_bijection(self):
        "One to one mapping. Each file in manifest has one corresponding engine_url."
        return [(f, f"json://{f}") for f in self.manifest_items]

    def map_fanout(self):
        "Each file in manifest goes to 2 output engine_urls."
        r = []
        for f in self.manifest_items:
            r.append((f, f"csv://{f}.csv"))
            r.append((f, f"ndjson://{f}.ndjson"))
        return r

    def map_collapse_in(self):
        "All files in manifest are processed into one output dataset. e.g. aggregation."
        aggregation_engine_url = "csv://results_summary.csv"
        return [(f, aggregation_engine_url) for f in self.manifest_items]


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

    def test_manifest_mapper_find_mapper_methods(self):
        class SuperMapper(AbstractManifestMapper):
            def map_xyz(self):
                pass

            def map_abc(self):
                pass

        manifest = Connect(engine_url=f"json://{TEST_DATA}/manifest_abcd.json")
        s = SuperMapper(manifest_dataset=manifest, field_name="more_files")

        key_names = s.methods_mapper.keys()
        self.assertEqual({"xyz", "abc"}, set(key_names))

    def test_manifest_full_map(self):

        manifest = Connect(engine_url=f"json://{TEST_DATA}/manifest_abcd.json")
        m = MagicMapper(manifest_dataset=manifest, field_name="more_files")

        expected = {
            "x.ndjson": {
                "bijection": ["json://x.ndjson"],
                "collapse_in": ["csv://results_summary.csv"],
                "fanout": ["csv://x.ndjson.csv", "ndjson://x.ndjson.ndjson"],
            },
            "y.ndjson": {
                "bijection": ["json://y.ndjson"],
                "collapse_in": ["csv://results_summary.csv"],
                "fanout": ["csv://y.ndjson.csv", "ndjson://y.ndjson.ndjson"],
            },
            "z.ndjson": {
                "bijection": ["json://z.ndjson"],
                "collapse_in": ["csv://results_summary.csv"],
                "fanout": ["csv://z.ndjson.csv", "ndjson://z.ndjson.ndjson"],
            },
        }
        self.assertEqual(expected, m.full_map)

    def test_manifest_iterate(self):

        manifest = Connect(engine_url=f"json://{TEST_DATA}/manifest_abcd.json")
        m = MagicMapper(manifest_dataset=manifest, field_name="more_files")

        for engine_set in m:
            # just test results for one mapping (fanout) for one manifest listed file
            if engine_set.manifest_item == "z.ndjson":
                expected = ["csv://z.ndjson.csv", "ndjson://z.ndjson.ndjson"]
                self.assertEqual(engine_set.fanout, expected)
                break
        else:
            raise ValueError("test item not found")

    def test_manifest_callable(self):
        """
        map_xxx() method becomes .xxx() method and is callable later
        """

        manifest = Connect(engine_url=f"json://{TEST_DATA}/manifest_abcd.json")
        m = MagicMapper(manifest_dataset=manifest, field_name="more_files")

        call_later = m.bijection

        # note - self.map_bijection() returns [(manifest_file, engine_url)..] and
        # .bijection just returns the engine_urls
        expected_engine_urls = ["json://x.ndjson", "json://y.ndjson", "json://z.ndjson"]

        # ... it's later now. Call it.
        self.assertEqual(expected_engine_urls, call_later())

    def test_abstract_manifest_mapper_not_shared(self):
        """
        class variable doesn't share variables between classes
        """

        class SeabedMapper(AbstractManifestMapper):
            def map_x(self):
                return [(f, f"json://{f}") for f in self.manifest_items]

        class SeabedSurvey(Model):
            manifest = Connect()
            mapper = SeabedMapper(manifest_dataset=manifest, field_name="more_files")
            x_files = Connect(engine_url=mapper.x)

            def __init__(self, manifest_file, **kwargs):
                super().__init__(**kwargs)
                self.manifest.update(engine_url=f"json://{manifest_file};encoding=utf-8-sig")

            def build(self):
                return

        s0 = SeabedSurvey(f"{TEST_DATA}/manifest_abcd.json")

        engine_urls = [x.engine_url for x in s0.x_files]
        self.assertEqual(engine_urls, ["json://x.ndjson", "json://y.ndjson", "json://z.ndjson"])

        # manifest file doesn't exist but old use of 'manifest_abcd.json' is still clinging
        # on to class variable.
        s0 = SeabedSurvey(f"{TEST_DATA}/manifest_does_not_exist.json")
        with self.assertRaises(ValueError) as context:
            engine_urls = [x.engine_url for x in s0.x_files]

        exception_msg = str(context.exception)
        self.assertTrue(exception_msg.endswith("which isn't readable"))

    def test_manifest_property_single_variable(self):
        """
        Use the AbstractManifestMapper to make a simple callable that will be resolved after the
        model is instantiated. It takes a value from a manifest file. It doesn't do a two way
        mapping. The LandAnimalsSurvey.bad_weather callable could be used to supply a value to a
        Connect parameter that isn't the engine_url but does support callables.
        """

        class ManifestProperty(AbstractManifestMapper):
            def bad_weather(self):
                # if 'london' is in the file name the weather will be bad
                return "london" in self.manifest_data["survey_weather"]

        class LandAnimalsSurvey(Model):
            manifest = Connect()
            build_attributes = ManifestProperty(manifest_dataset=manifest)
            bad_weather = build_attributes.bad_weather

            def __init__(self, manifest_file, **kwargs):
                super().__init__(**kwargs)
                self.manifest.update(engine_url=f"json://{manifest_file};encoding=utf-8-sig")

        survey = LandAnimalsSurvey(f"{TEST_DATA}/manifest_abcd.json")
        self.assertTrue(callable(survey.bad_weather))
        msg = 'The manifest contains "survey_weather": "london_weather.json" should should be true'
        self.assertTrue(survey.bad_weather(), msg)

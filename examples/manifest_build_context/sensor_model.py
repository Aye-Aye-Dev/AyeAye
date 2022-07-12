"""
Demo of ayeaye.common_pattern.build_context.manifest_build_context

Run it like this-

$ python sensor_model.py manifest_file.json

"""
import ayeaye
from ayeaye.common_pattern.build_context import manifest_build_context
from ayeaye.common_pattern.manifest import EngineFromManifest


class SensorInputs(ayeaye.Model):

    lookup = ayeaye.Connect(engine_url="csv:///data/lookup_table_{dataset_id}.csv")

    manifest = ayeaye.Connect(engine_url="json://{manifest_file}")
    sensor_readings = ayeaye.Connect(engine_url=EngineFromManifest(manifest, "sensor_readings", "csv"))

    def build(self):
        "demo to show manifest file variables being available to datasets"

        lookup_engine = self.lookup.engine_url

        # check it's running correctly!
        assert lookup_engine == "csv:///data/lookup_table_abc123.csv"

        self.log(f"dataset_id  is from the manifest file: {lookup_engine}")

        # multi connecotors can be given many files. In this example there are a list of .csv files
        # in the manifest file. Each becomes an ayeaye.connectors.CsvConnector
        sensor_engines = [sensor_connector.engine_url for sensor_connector in self.sensor_readings]
        mc_files = " ".join(sensor_engines)
        assert mc_files == "csv:///data/build/file_a.csv csv:///data/build/file_b.csv"

        self.log(f"multi_connector files: {mc_files}")


if __name__ == "__main__":
    with manifest_build_context():
        m = SensorInputs()
        m.go()

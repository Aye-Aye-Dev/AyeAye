from collections import defaultdict
from io import StringIO
import json
import os
import shutil
import tempfile
import unittest

import ayeaye
from ayeaye.common_pattern.parallel_model_runner import ExampleModelRunner

PROJECT_TEST_PATH = os.path.dirname(os.path.abspath(__file__))
EXAMPLE_CSV_PATH = os.path.join(PROJECT_TEST_PATH, "data", "deadly_creatures.csv")
EXAMPLE_TSV_PATH = os.path.join(PROJECT_TEST_PATH, "data", "monkeys.tsv")


class FindLongestAnimalName(ayeaye.PartitionedModel):
    """
    Find the longest common name in a collection of CSV/TSV files. Model suggests to executor how
    to break the task into parallel sub-tasks.
    """

    animals = ayeaye.Connect(
        engine_url=[
            f"csv://{EXAMPLE_CSV_PATH}",
            f"tsv://{EXAMPLE_TSV_PATH}",
        ]
    )
    animals_output = ayeaye.Connect(access=ayeaye.AccessMode.WRITE)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.common_names_max = []

    def build(self):
        pass

    def partition_plea(self):
        dataset_files_count = len(self.animals)
        return ayeaye.PartitionedModel.PartitionOption(
            minimum=1, maximum=dataset_files_count, optimal=dataset_files_count
        )

    def partition_slice(self, slice_count):
        target_method = "find_longest_name"  # this is the method subtasks should be running
        task_sets = defaultdict(list)
        for idx, dataset in enumerate(self.animals):
            task_id = idx % slice_count
            task_sets[task_id].append(dataset.engine_url)

        return [(target_method, {"engine_set": engine_set}) for engine_set in task_sets.values()]

    def partition_subtask_complete(self, subtask_method_name, subtask_kwargs, subtask_return_value):
        if subtask_method_name == "find_longest_name":
            self.common_names_max.append(subtask_return_value)

    def partition_complete(self):
        longest_animal_name = max(self.common_names_max, key=len)
        self.animals_output.data = longest_animal_name

    def find_longest_name(self, engine_set):
        """
        Find the longest common name in the self.animals[engine_url] dataset and return that
        string.
        """
        longest = ""
        for engine_url in engine_set:
            dataset = self.animals[engine_url]
            for row in dataset:
                if len(row.common_name) > len(longest):
                    longest = row.common_name

        return longest


class DistributedFakeWork(ayeaye.PartitionedModel):
    """
    Distribute a fake calculation and assemble the results.
    """

    # uses connector_resolver
    non_existant_data = ayeaye.Connect(engine_url="file://{hello_partitioned_context}")

    def build(self):
        pass

    def some_work(self, some_number):
        "add the worker id to a test number and append to fully resolved engine_url"
        some_data = self.non_existant_data.file_path + str(some_number)
        return some_data

    def partition_slice(self, _):
        target_method = "some_work"
        return [(target_method, {"some_number": x}) for x in range(10)]

    def partition_subtask_complete(self, subtask_method_name, subtask_kwargs, subtask_return_value):
        if not hasattr(self, "resultset"):
            self.resultset = []

        if subtask_method_name == "some_work":
            self.resultset.append(subtask_return_value)


class TestPartitionedModel(unittest.TestCase):
    def setUp(self):
        self._working_directory = None

    def tearDown(self):
        if self._working_directory and os.path.isdir(self._working_directory):
            shutil.rmtree(self._working_directory)

    def working_directory(self):
        self._working_directory = tempfile.mkdtemp()
        return self._working_directory

    def test_general_checks(self):
        m = FindLongestAnimalName()
        m.log_to_stdout = False

        output_file = "{}/animals_summary.json".format(self.working_directory())
        m.animals_output.update(engine_url=f"json://{output_file};indent=4")

        partition_option = m.partition_plea()
        self.assertEqual(2, partition_option.optimal)

        for split_size in [1, 2]:
            slices = m.partition_slice(split_size)
            self.assertEqual(split_size, len(slices))

            all_engine_urls = []
            for s in slices:
                self.assertEqual("find_longest_name", s[0])
                all_engine_urls.extend(s[1]["engine_set"])

            self.assertEqual(2, len(all_engine_urls))
            squashed_urls = " ".join(all_engine_urls)
            self.assertIn("deadly_creatures.csv", squashed_urls)
            self.assertIn("monkeys.tsv", squashed_urls)

    def test_happy_partitioned_path(self):
        m = FindLongestAnimalName()
        m.log_to_stdout = False

        output_file = "{}/animals_summary.json".format(self.working_directory())
        m.animals_output.update(engine_url=f"json://{output_file};indent=4")
        output_encoding = m.animals_output.encoding

        m.go()

        with open(output_file, "r", encoding=output_encoding) as f:
            output_data = json.load(f)

        expected_data = "Crown of thorns starfish"
        self.assertEqual(expected_data, output_data)

    def test_partitioned_connector_resolver(self):
        """
        Key -> Value pairs set on the global ConnectorResolver should be available to worker
        processes.
        Callables aren't yet supported.
        """
        build_context = {"hello_partitioned_context": "important_build_data.ndjson"}
        with ayeaye.connector_resolver.context(**build_context):
            m = DistributedFakeWork()
            m.log_to_stdout = False
            m.go()

        expected_results = set(["important_build_data.ndjson" + str(x) for x in range(10)])
        self.assertEqual(expected_results, set(m.resultset))

    def test_parallel_models(self):
        """
        Investigating how to run a load of models in parallel on a single machine using
        a `PartitionedModel`.
        """
        external_log = StringIO()

        build_context = {"greeting": "Hello model runner!"}
        with ayeaye.connector_resolver.context(**build_context):
            model_runner = ExampleModelRunner()
            model_runner.set_logger(external_log)
            model_runner.log_to_stdout = False
            model_runner.go()

        external_log.seek(0)
        all_the_logs = external_log.read()

        for expected_snippet, msg in [
            ("Running model A from position: 0", "ExampleModelRunner log"),
            ("This is Model C", "Target models output"),
            ("This is Model B with init arg: hi model B", "With init message"),
            ("From the build context: Hello model runner!", "With build context"),
        ]:
            self.assertIn(expected_snippet, all_the_logs, msg)

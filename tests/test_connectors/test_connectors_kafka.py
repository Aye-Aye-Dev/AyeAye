from datetime import datetime
import unittest

from ayeaye.connectors.kafka_connector import KafkaConnector

EXAMPLE_ENGINE_URL_0 = "kafka://localhost/topic=foobar;start=@(2019-05-15 08:00:00);end=@(2019-05-15 18:00:00);"
EXAMPLE_ENGINE_URL_1 = "kafka://localhost/topic=foobar;start=@(2019-05-15 15:50:18);end=@(2019-05-15 15:50:24);"
EXAMPLE_ENGINE_URL_2 = "kafka://localhost/topic=uas;start=@(2019-05-22 10:42:00);end=@(2019-05-22 10:44:00);"


class TestKafkaConnector(unittest.TestCase):

    def test_engine_decode(self):
        date_format = "%Y-%m-%d %H:%M:%S"
        c = KafkaConnector(engine_url=EXAMPLE_ENGINE_URL_0)
        bootstrap_server, topic, start_params, end_params = c._decode_engine_url()
        assert bootstrap_server == 'localhost'
        assert topic == 'foobar'
        assert start_params == datetime.strptime("2019-05-15 08:00:00", date_format)
        assert end_params == datetime.strptime("2019-05-15 18:00:00", date_format)

    @unittest.skip("Needs integration test suite")
    def test_partition_ranges(self):
        c = KafkaConnector(engine_url=EXAMPLE_ENGINE_URL_1)
        p_ranges = set([x for x in c._partition_ranges()])
        # my values right now, need to be aligned with test data when there's an integration env
        expected = {(0, 0, 233303), (1, 0, 133271), (2, 0, 133526)}
        assert expected == p_ranges

    @unittest.skip("Needs integration test suite")
    def test_subset_of_items(self):
        """
        Use a date range to get some items from a topic.
        """
        c = KafkaConnector(engine_url=EXAMPLE_ENGINE_URL_2)
        some_items = [x for x in c]
        # TODO there is a problem with .offsets_for_times(tx)
        # there should be a few hundred items here, not thousands
        assert len(some_items) == 100

    @unittest.skip("Needs integration test suite")
    def test_partition_ranges_2(self):
        c = KafkaConnector(engine_url=EXAMPLE_ENGINE_URL_2)
        p_ranges = set([x for x in c._partition_ranges()])
        # my values right now, need to be aligned with test data when there's an integration env
        expected = {(0, 0, 91)}
        assert expected == p_ranges

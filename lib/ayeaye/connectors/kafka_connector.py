'''
Created on 16 May 2019

@author: parkes25
'''
from datetime import datetime
from typing import Generator

try:
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.structs import OffsetAndTimestamp
except ModuleNotFoundError:
    pass

from ayeaye.connectors.base import DataConnector, AccessMode
from ayeaye.pinnate import Pinnate


class KafkaConnector(DataConnector):
    engine_type = 'kafka://'

    def __init__(self, *args, **kwargs):
        """
        Connector to Apache Kafka.
        Args: @see :class:`connectors.base.DataConnector`

        additional args for KafkaConnector
         None

        Connection information-
            engine_url format is kafka://bootstrap_server/topic=<topic>;[start params;][end params;]
        start and end params can be partitions with offsets or '@' notation to use dates.
        e.g. kafka://bionic/topic=foobar;start=@(2019-05-15 08:00:00);end=@(2019-05-15 18:00:00);
        """
        super().__init__(*args, **kwargs)

        # set by :method:`connect`
        self.bootstrap_server = self.topic = self.start_params = self.end_params = None
        self.start_p_offsets = self.end_p_offsets = None
        self.available_topics = None
        self.client = None

        # publicly readable
        self.stats = Pinnate({'added': 0})

        # used during read
        self.approx_position = None
        self.items_to_fetch = None

    def close_connection(self):
        if self.access == AccessMode.WRITE and self.client is not None:
            self.flush()

    def connect(self):
        if self.client is None:
            self.bootstrap_server, self.topic, self.start_params, self.end_params = \
                self._decode_engine_url()

            if self.access == AccessMode.READ:
                self.client = KafkaConsumer(bootstrap_servers=self.bootstrap_server)
                self._setup_consumer()

            elif self.access == AccessMode.WRITE:
                if self.start_params is not None or self.end_params is not None:
                    raise ValueError("Start and end offsets can't be set when writing")
                self.client = KafkaProducer(bootstrap_servers=self.bootstrap_server)

            else:
                raise NotImplementedError('Unknown access mode')

    def _setup_consumer(self):
        """
        prepare offset numbers etc. for reading from Topic
        """
        # <WTF> https://github.com/dpkp/kafka-python/issues/601
        self.available_topics = self.client.topics()
        # </WTF>

        # might as well use it
        assert self.topic in self.available_topics

        if (self.start_params is None) != (self.end_params is None):
            raise ValueError("Both start and end params must be set or both must be None")

        if self.start_params is None:
            # setup partitions to read through
            # TODO not checked with multiple partitions since inheriting from foxglove
            # An offset is assigned to make repeatability (via a locking file) possible later on.
            # and it's easier to terminate the fetch loop this way.
            p_id = self.client.partitions_for_topic(self.topic)
            topic_partitions = [TopicPartition(topic=self.topic, partition=p) for p in list(p_id)]
            starts = self.client.beginning_offsets(topic_partitions)
            ends = self.client.end_offsets(topic_partitions)

            self.start_p_offsets = {tp: OffsetAndTimestamp(
                offset=offset, timestamp=None) for tp, offset in starts.items()}
            self.end_p_offsets = {tp: OffsetAndTimestamp(
                offset=offset - 1, timestamp=None) for tp, offset in ends.items()}

        else:
            # TODO - this code was inherited from Foxglove and hasn't be checked through
            # setup start and end partitions and offsets
            # self.client.seek_to_beginning()
            # datetime is only start/end implemented
            assert isinstance(self.start_params, datetime) and isinstance(self.end_params, datetime)
            start = int(self.start_params.timestamp() * 1000)
            end = int(self.end_params.timestamp() * 1000)

            partitions = self.client.partitions_for_topic(self.topic)
            tx = {TopicPartition(topic=self.topic, partition=p): start
                  for p in list(partitions)}
            self.start_p_offsets = self.client.offsets_for_times(tx)

            # if you give a timestamp after the last record it returns None
            for tp, offset_details in self.start_p_offsets.items():
                if offset_details is None:
                    raise ValueError("Start date outside of available messages")

            tx = {TopicPartition(topic=self.topic, partition=p): end
                  for p in list(partitions)}
            self.end_p_offsets = self.client.offsets_for_times(tx)

            # as above - out of range, for end offset give something useful
            for tp, offset_details in self.end_p_offsets.items():
                if offset_details is None:
                    # go to last message. I'm not 100% sure this is correct
                    end_offsets = self.client.end_offsets([tp])
                    offset = end_offsets[tp] - 1
                    self.end_p_offsets[tp] = OffsetAndTimestamp(offset=offset, timestamp=None)

    def _decode_engine_url(self):
        """
        Returns:
            bootstrap_server, topic, start_params, end_params
            bootstrap_server and topic are (str)
            start_params, end_params are (None), (datetime) or (mixed) - not implemented
                                but will be partition+offsets pairs
        """
        date_format = "%Y-%m-%d %H:%M:%S"
        r = dict(topic=None, start=None, end=None)
        s_url = self.engine_url[len(self.__class__.engine_type):]
        bootstrap_server, r_url = s_url.split('/', 1)
        for param_section in r_url.split(';'):
            if len(param_section) == 0:
                continue
            k, v = param_section.split('=', 1)
            if k in r:
                r[k] = v
        # resolve to dates if needed
        # partition+offset not implemented so start and end must be None or start with @
        # to resolve to a datetime.
        for position in ('start', 'end'):
            p_marker = r[position]
            if p_marker is not None:
                assert p_marker.startswith('@(') and p_marker.endswith(')')
                date_str = p_marker[2:-1]
                r[position] = datetime.strptime(date_str, date_format)

        return bootstrap_server, r['topic'], r['start'], r['end']

    def schema(self):
        raise NotImplementedError("TODO")

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def _partition_ranges(self) -> Generator:
        """
        yield partition (int), start_offset (int), end_offset (int)
        for range given in self.engine_url
        """
        self.connect()
        for topic_partition, start_offset_time in self.start_p_offsets.items():
            end_offset_time = self.end_p_offsets[topic_partition]
            yield topic_partition.partition, start_offset_time.offset, end_offset_time.offset

    @property
    def data(self) -> Generator:
        """
        Generator yielding just the value of the record from Kafka.
        Value is made into Pinnate object.

        See https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
        useful attribs include
        m.offset, m.partition, m.timestamp, m.key, m.value
        """
        # Not using a consumer group and setting partitions manually so it's a smaller
        # jump to make this deterministic/repeatable with multiple workers later on.

        self.connect()

        self.approx_position = 0
        for partition_id, start_offset, end_offset in self._partition_ranges():

            # TODO - confirm this can never jump to another partition
            tp = TopicPartition(topic=self.topic, partition=partition_id)
            self.client.assign([tp])

            self.items_to_fetch = end_offset - start_offset
            self.client.seek(tp, start_offset)

            if self.items_to_fetch <= 0:
                msg = f"Invalid offsets {start_offset}:{end_offset} for partition {partition_id}"
                raise ValueError(msg)

            for m in self.client:

                self.approx_position += 1
                yield Pinnate(data=m.value)

                if end_offset is not None and m.offset >= end_offset:
                    break

    def add(self, data, partition=None):
        """
        Write message to topic.
        @param data: (str)
        @param partition: (int) Kafka partition. Not yet implemented.
        """
        # TODO expand data to include binary and instance of :class:`Pinnate` but needs a way of
        # de-serialising on retrieve.

        if self.access != AccessMode.WRITE:
            raise ValueError("Write attempted on dataset opened in READ mode.")

        if partition is not None:
            raise NotImplementedError("Placeholder value, not implemented yet")

        if not isinstance(data, str):
            raise ValueError("data isn't an accepted type. Only (str) is accepted.")

        self.connect()

        # TODO use futures
        self.client.send(self.topic, value=bytes(data, 'utf-8'))
        self.stats.added += 1

    def flush(self):
        """
        Ensure all messages have been sent to Kafka
        """
        if self.access != AccessMode.WRITE:
            raise ValueError("Flush attempted on dataset opened in READ mode.")

        # TODO futures and performance stats
        self.client.flush()

    @property
    def progress(self):
        if self.access != AccessMode.READ \
                or self.items_to_fetch is None \
                or self.approx_position is None:
            return None

        return self.approx_position / self.items_to_fetch

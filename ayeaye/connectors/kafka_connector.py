'''
Created on 16 May 2019

@author: parkes25
'''
from datetime import datetime
from typing import Generator

try:
    from kafka import KafkaConsumer, TopicPartition
    from kafka.structs import OffsetAndTimestamp
except:
    pass

from ayeaye.connectors.base import DataConnector
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

    def connect(self):
        if self.client is None:
            self.bootstrap_server, self.topic, self.start_params, self.end_params = \
                self._decode_engine_url()
            self.client = KafkaConsumer(bootstrap_servers=self.bootstrap_server)

            # <WTF> https://github.com/dpkp/kafka-python/issues/601
            self.available_topics = self.client.topics()
            # </WTF>
            
            # might as well use it
            assert self.topic in self.available_topics

            # setup start and end partitions and offsets
#             self.client.seek_to_beginning()
            # datetime is only start/end implemented
            assert isinstance(self.start_params, datetime) and isinstance(self.end_params, datetime)
            start = int(self.start_params.timestamp() * 1000)
            end = int(self.end_params.timestamp() * 1000)

            partitions = self.client.partitions_for_topic(self.topic)
            tx = {TopicPartition(topic=self.topic, partition=p):start
                  for p in list(partitions)}
            self.start_p_offsets = self.client.offsets_for_times(tx)

            # if you give a timestamp after the last record it returns None
            for tp, offset_details in self.start_p_offsets.items():
                if offset_details is None:
                    raise ValueError("Start date outside of available messages")

            tx = {TopicPartition(topic=self.topic, partition=p):end
                  for p in list(partitions)}
            self.end_p_offsets = self.client.offsets_for_times(tx)
            
            # as above - out of range, for end offset give something useful
            for tp, offset_details in self.end_p_offsets.items():
                if offset_details is None:
                    # go to last message. I'm not 100% sure this is correct
                    end_offsets = self.client.end_offsets([tp])
                    offset = end_offsets[tp]-1
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
            k,v = param_section.split('=', 1)
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
        self.connect()

        for partition_id, start_offset, end_offset in self._partition_ranges():
            # TODO - confirm this can never jump to another partition
            tp = TopicPartition(topic=self.topic, partition=partition_id)
            items_to_fetch = end_offset-start_offset
            self.client.assign([tp])
            self.client.seek(tp, start_offset)

            if items_to_fetch <= 0:
                msg = f"Invalid offsets {start_offset}:{end_offset} for partition {partition_id}"
                raise ValueError(msg)

            for m in self.client:

                #dt = datetime.utcfromtimestamp(m.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')
                #msg = "{}-{} {} key:{} value:{}".format(m.offset, m.partition, dt, m.key, m.value)
                #if m.offset == 100000:
                #    print("now")
                #print(msg)
                yield Pinnate(data=m.value)

                if m.offset >= end_offset:
                    break

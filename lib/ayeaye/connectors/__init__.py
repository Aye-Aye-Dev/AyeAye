from inspect import isclass

from .base import DataConnector
from .bigquery import BigQueryConnector
from .csv_connector import CsvConnector, TsvConnector
from .elasticsearch_connector import ElasticsearchConnector
from .fake import FakeDataConnector
from .flowerpot import FlowerPotConnector
from .gcs_flowerpot import GcsFlowerpotConnector, FlowerpotEngine
from .json_connector import JsonConnector
from .kafka_connector import KafkaConnector
from .ndjson_connector import NdjsonConnector
from .parquet_connector import ParquetConnector
from .sqlalchemy_database import SqlAlchemyDatabaseConnector
from .uncooked_connector import UncookedConnector

class ConnectorPluginsRegistry:

    def __init__(self):
        self.registered_connectors = [] # list of classes, not instances - publicly readable
        self.reset()

    def reset(self):
        "set registered connectors to just those that are built in"
        self.registered_connectors = [BigQueryConnector, CsvConnector, FlowerPotConnector,
                                      FakeDataConnector, KafkaConnector, ParquetConnector,
                                      TsvConnector, SqlAlchemyDatabaseConnector, JsonConnector,
                                      NdjsonConnector, ElasticsearchConnector, UncookedConnector,
                                      ]

    def register_connector(self, connector_cls):
        """
        @param connector_cls ():
        """
        if not isclass(connector_cls) or not issubclass(connector_cls, DataConnector):
            msg = "'connector_cls' should be a class (not object) and subclass of DataConnector"
            raise TypeError(msg)

        # MAYBE - a mechanism to specify the position/priority of the class
        self.registered_connectors.append(connector_cls)


# global registry
connector_registry = ConnectorPluginsRegistry()

def connector_factory(engine_url):
    """
    return a subclass of DataConnector
    @param engine_url (str):
    """
    # TODO - these really need to be on demand and possibly outside this project
    engine_type = engine_url.split('://', 1)[0] + '://'
    for connector_cls in connector_registry.registered_connectors:
        if isinstance(connector_cls.engine_type, list):
            supported_engines = connector_cls.engine_type
        else:
            supported_engines = [connector_cls.engine_type]

        if engine_type in supported_engines:
            return connector_cls

    raise NotImplementedError(f"Unknown engine in url:{engine_url}")


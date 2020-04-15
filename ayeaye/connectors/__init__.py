from .bigquery import BigQueryConnector
from .csv_connector import CsvConnector, TsvConnector
from .fake import FakeDataConnector
from .flowerpot import FlowerPotConnector
from .gcs_flowerpot import GcsFlowerpotConnector, FlowerpotEngine
from .json_connector import JsonConnector
from .kafka_connector import KafkaConnector
from .parquet_connector import ParquetConnector
from .sqlalchemy_database import SqlAlchemyDatabaseConnector


def connector_factory(engine_url):
    """
    return a subclass of DataConnector
    @param engine_url (str):
    """
    engine_type = engine_url.split('://', 1)[0] + '://'
    for connector_cls in [BigQueryConnector, CsvConnector, FlowerPotConnector,
                          FakeDataConnector, KafkaConnector, ParquetConnector,
                          TsvConnector, SqlAlchemyDatabaseConnector, JsonConnector]:
        if isinstance(connector_cls.engine_type, list):
            supported_engines = connector_cls.engine_type
        else:
            supported_engines = [connector_cls.engine_type]

        if engine_type in supported_engines:
            return connector_cls

    raise NotImplementedError(f"Unknown engine in url:{engine_url}")


__all__ = ['connector_factory', 'FakeDataConnector']

from inspect import isclass

from .base import DataConnector
from .bigquery import BigQueryConnector
from .csv_connector import CsvConnector, TsvConnector
from .elasticsearch_connector import ElasticsearchConnector
from .fake import FakeDataConnector
from .json_connector import JsonConnector
from .kafka_connector import KafkaConnector
from .ndjson_connector import NdjsonConnector
from .parquet_connector import ParquetConnector
from .restful_connector import RestfulConnector
from .sqlalchemy_database import SqlAlchemyDatabaseConnector
from .uncooked_connector import UncookedConnector

from ayeaye.ignition import Ignition, EngineUrlCase, EngineUrlStatus
from ayeaye.connectors.engine_type_modifiers import engine_type_modifier_factory


class ConnectorPluginsRegistry:
    def __init__(self):
        self.registered_connectors = []  # list of classes, not instances - publicly readable
        self.reset()

    def reset(self):
        "set registered connectors to just those that are built in"
        self.registered_connectors = [
            BigQueryConnector,
            CsvConnector,
            FakeDataConnector,
            KafkaConnector,
            ParquetConnector,
            TsvConnector,
            SqlAlchemyDatabaseConnector,
            JsonConnector,
            NdjsonConnector,
            ElasticsearchConnector,
            UncookedConnector,
            RestfulConnector,
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
    @param engine_url (str):
        format is [modifier+[...modifier_n+]]<engine_type>://<engine_specific_parameters>
    @return a subclass of :class:`DataConnector`
    """
    engine_type_separator = "://"
    if isinstance(engine_url, str) and engine_type_separator not in engine_url:
        # The engine type is only available after the context has been resolved. Don't error here
        # if resolution isn't yet possible.
        ignition = Ignition(engine_url)
        try:
            status, e_url = ignition.engine_url_at_state(EngineUrlCase.FULLY_RESOLVED)
            if status == EngineUrlStatus.OK:
                engine_url = e_url
        except ValueError:
            # Full resolve not available yet. It could be possible to use a partially
            # resolved url if this current behaviour isn't good enough.
            pass

    if engine_type_separator in engine_url:
        engine_type = engine_url.split(engine_type_separator, 1)[0] + engine_type_separator

        modifier_labels = None
        if "+" in engine_type:
            # engine type modifiers are being used. e.g. the 'gz' in `gz+ndjson://`
            engine_parts = engine_type.split("+")
            modifier_labels = engine_parts[:-1]
            engine_type = engine_parts[-1]

        for connector_cls in connector_registry.registered_connectors:
            if isinstance(connector_cls.engine_type, list):
                supported_engines = connector_cls.engine_type
            else:
                supported_engines = [connector_cls.engine_type]

            if engine_type in supported_engines:
                if modifier_labels:
                    # find a suitable modifier - it needs to support this connector class
                    # and the modifiers
                    modified_connector_cls = engine_type_modifier_factory(
                        connector_cls=connector_cls,
                        modifier_labels=modifier_labels,
                    )

                    # build a DataConnector like class that can be treated just like a normal
                    # data connector.
                    dynamic_dc_cls = modified_connector_cls.apply(
                        connector_cls=connector_cls,
                        modifier_labels=modifier_labels,
                    )

                    return dynamic_dc_cls

                return connector_cls

    raise NotImplementedError(f"Unknown engine in url:{engine_url}")

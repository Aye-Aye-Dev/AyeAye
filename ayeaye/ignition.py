from enum import Enum


class EngineUrlCase(Enum):
    RAW = 'raw'  # unprocessed first sight of engine_url - e.g. as passed to :class:`Connect`
    WITHOUT_SECRETS = 'without_secrets'  # everything needed to connect to dataset apart from secrets
    FULLY_RESOLVED = 'fully_resolved'  # has everything needed to connect to dataset


class EngineUrlStatus(Enum):
    OK = 'ok'  # has been resolved
    NOT_AVAILABLE = 'not_available'  # will never be available
    NOT_YET_AVAILABLE = 'not_yet_available'  # deferred. waiting on something
    UNKNOWN = 'unknown'


class Ignition:
    """
    Control engine_urls.
    - common functions like breaking the url into parts
    - resolving variables at runtime
    - translating path separators so engine_urls are independent of the operating system.

    'engine_urls' are the connection strings used by all subclasses of :class:`DataConnector`. 
    """

    def __init__(self, engine_url):
        """
        @param engine_url: (str) an unresolved engine_url or one that doesn't need any resolution.
                An unresolved engine_url is one containing variables that need to be substituted
                before the engine_url can be used to connect to the dataset.
                e.g. "mysql://root:{env_secret_password}@localhost/my_database"
        """
        self._engine_url_state = {EngineUrlCase.RAW: engine_url}

    def engine_url_at_state(self, engine_url_case):
        """
        Get the engine_url in a specific case. e.g. raw, without secrets or fully resolved.

        @param engine_url_case: (EngineUrlCase)
        @return: (EngineUrlStatus, str)
        """
        assert isinstance(engine_url_case, EngineUrlCase)
        if engine_url_case in self._engine_url_state:
            return EngineUrlStatus.OK, self._engine_url_state[engine_url_case]

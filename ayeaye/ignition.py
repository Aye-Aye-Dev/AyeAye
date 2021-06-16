from enum import Enum
import os.path

from ayeaye.connect_resolve import connector_resolver
from ayeaye.pinnate import Pinnate


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

    This class is only used internally by subclasses of :class:`DataConnector`.
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

        @raise ValueError: from connector_resolver
        @param engine_url_case: (EngineUrlCase)
        @return: (EngineUrlStatus, str or None)
        """
        assert isinstance(engine_url_case, EngineUrlCase)

        if engine_url_case == EngineUrlCase.WITHOUT_SECRETS:
            raise NotImplementedError("TODO")

        if engine_url_case not in self._engine_url_state:
            raw_e_url = self._engine_url_state[EngineUrlCase.RAW]
            if connector_resolver.needs_resolution(raw_e_url):
                # local resolution is still needed
                resolved = connector_resolver.resolve(raw_e_url)
                self._engine_url_state[EngineUrlCase.FULLY_RESOLVED] = resolved
            else:
                # nothing to resolve so fully_resolved = raw
                self._engine_url_state[EngineUrlCase.FULLY_RESOLVED] = \
                    self._engine_url_state[EngineUrlCase.RAW]

        if engine_url_case in self._engine_url_state:
            return EngineUrlStatus.OK, self._engine_url_state[engine_url_case]

        return EngineUrlStatus.NOT_AVAILABLE, None

    @staticmethod
    def _decode_filesystem_engine_url(engine_url, required_args=None, optional_args=None):
        """
        For connectors that access files on a local filesystem break the URL into useful parts.

        URL format is
        engine_type://filesystem_path[;arg_0=argv_0[;...]]

        Raises value error if there is anything odd in the URL.

        @param engine_url: (str)
        @param required_args (list of str)
        @param optional_args (list of str)
        @return: (Pinnate) with .file_path and .engine_type
                                and optional and required args
        """
        if optional_args is None:
            optional_args = []

        if required_args is None:
            required_args = []

        all_args = optional_args + required_args

        # TODO cls.engine_type could be a list. It's normally a string.
        engine_type, remaining_url = engine_url.split("://", 1)
        path_plus = remaining_url.split(';')
        file_path = path_plus[0]
        d = {'file_path': file_path,
             'engine_type': engine_type,
             }
        if len(path_plus) > 1:
            for arg in path_plus[1:]:
                k, v = arg.split("=", 1)
                if k not in all_args:
                    raise ValueError(f"Unknown option: {k}")
                d[k] = v

        if os.path.sep != '/':
            # urrgh, Windoze
            d['file_path'] = d['file_path'].replace('/', os.path.sep)

        return Pinnate(d)

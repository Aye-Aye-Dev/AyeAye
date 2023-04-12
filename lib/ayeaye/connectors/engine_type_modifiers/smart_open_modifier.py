try:
    from smart_open import open as smart_open
except ModuleNotFoundError:
    pass

from ayeaye.connectors.base import FileBasedConnector
from ayeaye.connectors.engine_type_modifiers.abstract_modifier import AbstractEngineTypeModifier


class SmartOpenModifier(AbstractEngineTypeModifier):
    """
    Use a sub-set of smart-open (https://pypi.org/project/smart-open/) to-
    - open s3 blobs
    - transparently compress and de-compress
    - expand file patterns using wild cards

    for all sub-classes of :class:`FilesystemConnector`.

    Implementation : This class is used to override methods in :class:`FilesystemConnector`.
    """

    @staticmethod
    def provides_support(connector_cls, modifier_labels):
        supported_labels = set(["gz", "s3"])
        proposed = set(modifier_labels)

        supported = proposed.issubset(supported_labels) and issubclass(
            connector_cls, FileBasedConnector
        )
        return supported

    def _open(self, *args, **kwargs):
        """
        Overrides :method:`FilesystemConnector.connect` with one using Smart Open's open.
        """
        smart_open_kwargs = {}
        if "gz" in self.requested_modifier_labels:
            smart_open_kwargs["compression"] = ".gz"
        else:
            smart_open_kwargs["compression"] = "disable"

        return smart_open(*args, **kwargs, **smart_open_kwargs)

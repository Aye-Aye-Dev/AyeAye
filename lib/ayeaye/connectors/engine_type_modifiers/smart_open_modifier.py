import os

try:
    from smart_open import open as smart_open
    import boto3
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

    def __init__(self):
        # lazy load variables
        self.__remote_file_attribs = None
        self.__s3_client = None

    @staticmethod
    def provides_support(connector_cls, modifier_labels):
        supported_labels = set(["gz", "s3"])
        proposed = set(modifier_labels)

        supported = proposed.issubset(supported_labels) and issubclass(
            connector_cls, FileBasedConnector
        )
        return supported

    @property
    def _s3_resource(self):
        return "s3" in self.requested_modifier_labels

    @property
    def _s3_client(self):
        if self.__s3_client is None:
            self.__s3_client = boto3.client("s3")

        return self.__s3_client

    @property
    def _remote_file_attribs(self):
        """
        @return: dict or None (if file doesn't exist)
            dict-
            'file_size' : int or None (if not available)
        """
        assert self._s3_resource, "should only be used in S3 mode"

        if self.__remote_file_attribs is None:
            bucket_name, obj_key = self.file_path.split("/", 1)

            try:
                r = self._s3_client.get_object_attributes(
                    Bucket=bucket_name,
                    Key=obj_key,
                    ObjectAttributes=["ObjectSize"],
                )
            except self._s3_client.exceptions.NoSuchKey:
                return

            self.__remote_file_attribs = {"file_size": r.get("ObjectSize")}

        return self.__remote_file_attribs

    def _open(self, *args, **kwargs):
        """
        Overrides :method:`FilesystemConnector.connect` with one using Smart Open's open.
        """
        smart_open_kwargs = {}
        if "gz" in self.requested_modifier_labels:
            smart_open_kwargs["compression"] = ".gz"
        else:
            smart_open_kwargs["compression"] = "disable"

        if self._s3_resource:
            # first arg is always the file path. Pre-fix this for smart open
            args = tuple(["s3://" + args[0]] + list(args[1:]))

        return smart_open(*args, **kwargs, **smart_open_kwargs)

    @property
    def datasource_exists(self):
        """
        Returns:
            (bool) if the datasource referred to in self.engine_url exists.
        """
        if self._s3_resource:
            return self._remote_file_attribs is not None

        return os.path.exists(self.file_path)

    def _get_file_size(self):
        """
        @return: int or None if not available
        """
        if self.datasource_exists:
            if self._s3_resource:
                return self._remote_file_attribs.get("file_size")

            return os.stat(self.file_path).st_size
        return None

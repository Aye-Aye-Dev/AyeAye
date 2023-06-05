from datetime import datetime, timezone
import os

try:
    from smart_open import open as smart_open
    import boto3
except ModuleNotFoundError:
    pass

from ayeaye.connectors.base import FileBasedConnector, FilesystemEnginePattern
from ayeaye.connectors.engine_type_modifiers.abstract_modifier import AbstractEngineTypeModifier
from ayeaye.connectors.engine_type_modifiers.utils import s3_pattern_match
from ayeaye.ignition import EngineUrlCase, EngineUrlStatus


class SmartOpenEnginePattern(FilesystemEnginePattern):
    """
    Use wildcards to pattern match files and directories. @see :class:`AbstractExpandEnginePattern`.

    The class adds S3 pattern expansion to the filesystem expansion provided by
    :class:`FilesystemEnginePattern`.
    """

    def expand_pattern(self):
        if not self.data_connector._s3_resource:
            # Not working with an S3 hosted file so must be using a local filesystem
            # so use the parent's method.
            return super().expand_pattern()

        status, e_url = self.data_connector.ignition.engine_url_at_state(
            EngineUrlCase.FULLY_RESOLVED
        )
        assert status == EngineUrlStatus.OK, "Super class should have caught this."

        # strip engine type
        # for now, they all need to have the same engine_type. Maybe engine_url starts
        # with `://` for auto detect based on file name.
        engine_type, engine_path_pattern = e_url.split("://", 1)

        if "?" in engine_path_pattern:
            raise NotImplementedError("TODO: Sorry, '?' isn't yet supported.")

        if "/" not in engine_path_pattern:
            raise ValueError(f"Bucket name not found in: {e_url}")

        bucket_name, obj_key_pattern = engine_path_pattern.split("/", 1)

        # the S3 API can only filter by the start of the file/object name. The rest is done with
        # a regular expression.
        prefix, matcher = s3_pattern_match(obj_key_pattern)

        s3_client = self.data_connector._s3_client
        continuation_token = None
        s3_kwargs = {"Bucket": bucket_name, "Prefix": prefix}
        engine_url = []
        while True:
            response = s3_client.list_objects_v2(**s3_kwargs)
            content = response.get("Contents", [])
            for c in content:
                engine_file = c["Key"]
                if matcher(engine_file):
                    engine_url.append(f"{engine_type}://{engine_file}")

            continuation_token = response.get("NextContinuationToken", None)
            if continuation_token:
                # 2nd page onwards
                s3_kwargs["ContinuationToken"] = continuation_token
            else:
                # end of pages
                break

        return engine_url


class SmartOpenModifier(AbstractEngineTypeModifier):
    """
    Use a sub-set of smart-open (https://pypi.org/project/smart-open/) to-
    - open s3 blobs
    - transparently compress and de-compress
    - expand file patterns using wild cards

    for all sub-classes of :class:`FilesystemConnector`.

    Implementation : This class is used to override methods in :class:`FilesystemConnector`.
    """

    engine_pattern_expander_cls = SmartOpenEnginePattern

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

    @property
    def last_modified(self):
        """
        Returns:
            (UTC `datetime.datetime`) of file, or None if file does not exist
        """
        if not self.datasource_exists:
            return None

        if self._s3_resource:
            raise NotImplementedError("TODO: S3, get from :method:`_remote_file_attribs`")

        timestamp = os.path.getmtime(self.file_path)
        last_modified = datetime.utcfromtimestamp(timestamp).replace(tzinfo=timezone.utc)
        return last_modified

    def auto_create_directory(self):
        if self._s3_resource:
            # I'm pretty sure you don't need to do this with s3/gcs
            return

        self._auto_create_directory()

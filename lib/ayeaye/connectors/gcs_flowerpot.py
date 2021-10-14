from tempfile import TemporaryFile
from typing import Generator, Tuple

try:
    from google.auth.credentials import Credentials
    from google.cloud import storage
    from google.cloud.storage import Blob
except ModuleNotFoundError:
    pass

from ayeaye.connectors.base import DataConnector
from ayeaye.connectors.flowerpot import FlowerpotEngine


class GcsFlowerpotConnector(DataConnector):
    engine_type = 'gs+flowerpot://'
    optional_args = {'credentials': None}

    def __init__(self, *args, **kwargs):
        """
        Connector to Datalab :class:`Flowerpot` stored in Google Cloud Storage (GCS) buckets.
        Args: @see :class:`connectors.base.DataConnector`

        additional args for GcsFlowerpotConnector
         'credentials' : (dict) for access within Google Cloud Platform

            engine_url format is gs+flowerpot://[project.]<bucket>/<path>
                    e.g. gs+flowerpot://my_project.my_bucket/some/data/file.flowerpot
                    where gs://my_bucket/some/data/file.flowerpot is the gsutil style URL
        """
        super().__init__(*args, **kwargs)

        self.project_name, self.bucket_name, self.flowerpot_path = \
            split_gcs_uri(kwargs['engine_url'])
        self.bucket = get_gcs_bucket(self.bucket_name, self.project_name, self.credentials)
        self._flowerpot = None  # loaded on demand

    def _get_blob(self):
        """Get flowerpot object from its remote path as a GCS blob"""
        return self.bucket.get_blob(self.flowerpot_path)

    @property
    def flowerpot(self) -> 'LazyFlowerpotReader':
        if self._flowerpot is None:
            flowerpot = self._download_flowerpot()
            self._flowerpot = FlowerpotEngine.from_file(flowerpot)
        return self._flowerpot

    def _download_flowerpot(self) -> TemporaryFile:
        flowerpot_file = TemporaryFile('w+b')
        self._get_blob().download_to_file(flowerpot_file)
        flowerpot_file.seek(0)
        return flowerpot_file

    @property
    def data(self) -> Generator[object, None, None]:
        return self.flowerpot.items()

    @property
    def schema(self):
        return None

    def connect(self):
        # looking at this property forces lazy download
        assert self.flowerpot

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")


def split_gcs_uri(gcs_uri: str) -> Tuple[str, str]:
    """
    Split a Google Cloud Storage URL into component (bucket and key) parts for use with GCS API.
    Args:
        gcs_uri: The `gs://` URI to split
    Returns:
        tuple: (bucket, key) pair of GCS bucket name and GCS key of item in URL.
    """
    url_prefix = GcsFlowerpotConnector.engine_type
    assert gcs_uri.startswith(
        url_prefix), "Google Cloud Storage URIs should start with {}".format(url_prefix)
    proj_bucket, path = tuple(gcs_uri[len(url_prefix):].split('/', 1))
    if '.' in proj_bucket:
        project, bucket = proj_bucket.split('.', 1)
        return project, bucket, path
    return None, proj_bucket, path


def get_gcs_bucket(bucket_name, project=None, credentials=None):
    """
    Convenience method for getting a GCS Bucket() object from a bucket name
    :param: bucket_name (str)
    :param: project (str)
    :param: credentials (google.auth.credentials,Credentials)
    """
    storage_client = storage.Client(project, credentials)
    return storage_client.get_bucket(bucket_name)

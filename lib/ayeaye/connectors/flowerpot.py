import tarfile
from typing import Generator, List

try:
    import ndjson
except ModuleNotFoundError:
    pass

from ayeaye.connectors.base import DataConnector
from ayeaye.pinnate import Pinnate


class FlowerpotEngine:
    """File-access Interface to 'Flowerpot' style ND-JSON tarballs."""

    def __init__(self, tar_file):
        self._tar_file = tar_file

    @staticmethod
    def from_filename(filename) -> 'LazyFlowerpotReader':
        tf = tarfile.open(filename, mode='r:gz')
        return FlowerpotEngine(tf)

    @staticmethod
    def from_file(file_object) -> 'LazyFlowerpotReader':
        tf = tarfile.open(fileobj=file_object, mode='r:gz')
        return FlowerpotEngine(tf)

    @property
    def file_names(self) -> List[str]:
        """Get all filenames contained in a flowerpot tarball"""
        file_names = [f.name for f in self._tar_file.getmembers() if f.isfile()]
        return file_names

    def _extract_ndjson_to_list(self, filename) -> List[object]:
        """Deserialize an entire ndjson file to a python list"""
        with self._tar_file.extractfile(filename) as f:
            byte_string = f.read()
            return FlowerpotEngine._deserialize_ndjson_string(byte_string)

    def file_handles(self) -> Generator[object, None, None]:
        """
        Generator yielding (file_name (str), file_handle) for all files in this flowerpot.
        It's polite for the user of this generator to .close() the file handle.
        """
        for file_name in self.file_names:
            fh = self._tar_file.extractfile(file_name)
            yield file_name, fh

    @staticmethod
    def _deserialize_ndjson_string(byte_string) -> List[object]:
        """
        Deserialize the contents of a newline-delimited JSON string to a list
        Args:
            byte_string: The NDJSON contents to be deserialized
        Returns:
            list: Each individual JSON entry deserialized as Python objects
        """
        utf8_string = str(byte_string, 'utf-8')
        content = ndjson.loads(utf8_string)
        return content

    def items(self, file_name=None) -> Generator[object, None, None]:
        """
        Args:
            file_name: (str or list of str) file(s) within flowerpot if not
                        given, all files will be used.
        Generator returning each json object in a Flowerpot (across all individual files)
        """
        if file_name:
            selected_files = file_name if isinstance(file_name, list) else [file_name, ]
        else:
            selected_files = self.file_names

        for file in selected_files:
            yield from self._items_in_file(file)

    def _items_in_file(self, file) -> object:
        """Generator returning each json object in an ndjson file in :class:`Pinnate` form."""
        for deserialized_json_object in self._extract_ndjson_to_list(file):
            yield Pinnate(data=deserialized_json_object)


class FlowerPotConnector(DataConnector):
    engine_type = 'flowerpot://'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._flowerpot = None

    def connect(self):
        if self._flowerpot is None:
            file_path = self.engine_url.split(self.engine_type)[1]
            self._flowerpot = FlowerpotEngine.from_filename(file_path)

    @property
    def flowerpot(self) -> object:
        if self._flowerpot is None:
            self.connect()
        return self._flowerpot

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def query(self, **kwargs):
        """
        get a subset of :method:`data`.
        Allowed options-
        table : get rows from all files that start with that string followed by an underscore,
                followed by a number
        """
        table = kwargs.pop('table', None)
        if not table:
            raise NotImplementedError("Only 'table' has been implemented.")

        self.connect()
        # could use a regex
        selected_files = []
        for file in self._flowerpot.file_names:
            if not file.startswith(table):
                continue
            suffix = file[len(table):]
            if suffix[0] != '_':
                continue
            numeric_part = suffix[1:].split('.', 1)[0]
            if not numeric_part.isdecimal():
                continue
            selected_files.append(file)

        if len(selected_files) == 0:
            raise ValueError("Table doesn't exist")

        return self._flowerpot.items(file_name=selected_files)

    @property
    def data(self) -> Generator:
        self.connect()
        return self._flowerpot.items()

    @property
    def schema(self):
        return None

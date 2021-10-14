'''
Created on 17 Dec 2020

@author: si
'''
import os

try:
    import ndjson
except ModuleNotFoundError:
    pass

from ayeaye.connectors.base import DataConnector, AccessMode
from ayeaye.pinnate import Pinnate


class NdjsonConnector(DataConnector):
    engine_type = 'ndjson://'

    def __init__(self, *args, **kwargs):
        """
        Connector to Newline Delimited JSON files (http://ndjson.org/).

        ndjson is one json document per line.

        For args: @see :class:`connectors.base.DataConnector`

        additional args for NdjsonConnector
         None

        Connection information-
            engine_url format is
            ndjson://<filesystem absolute path>[;encoding=<character encoding>]
        e.g. ndjson:///data/my_project/the_data.json;encoding=latin-1
        """
        super().__init__(*args, **kwargs)

        self._reset()

        if self.access == AccessMode.READWRITE:
            raise NotImplementedError('READWRITE access not yet implemented')

    def _reset(self):
        self.file_handle = None
        self.reader = None
        self.writer = None
        self._encoding = None
        self._engine_params = None
        self.file_size = None
        self.approx_position = 0

    @property
    def engine_params(self):
        """
        @return: (Pinnate) with .file_path
                        and optional: .encoding
        """
        if self._engine_params is None:
            self._engine_params = self.ignition._decode_filesystem_engine_url(
                self.engine_url,
                optional_args=['encoding']
            )

            if 'encoding' in self._engine_params:
                self._encoding = self.engine_params.encoding

        return self._engine_params

    @property
    def encoding(self):
        """
        default encoding. 'sig' means don't include the unicode BOM
        """
        if self._encoding is None:
            ep = self.engine_params
            self._encoding = ep.encoding if 'encoding' in ep else 'utf-8-sig'

        return self._encoding

    def close_connection(self):
        if self.file_handle is not None:
            self.file_handle.close()
        self._reset()

    def connect(self):
        if self.reader is None and self.writer is None:

            if self.access == AccessMode.READ:
                self.file_handle = open(self.engine_params.file_path, 'r', encoding=self.encoding)
                self.file_size = os.stat(self.engine_params.file_path).st_size
                self.reader = ndjson.reader(self.file_handle)

            elif self.access == AccessMode.WRITE:
                self.file_handle = open(self.engine_params.file_path, 'w', encoding=self.encoding)
                self.writer = ndjson.writer(self.file_handle)

            else:
                raise ValueError('Unknown access mode')

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        self.connect()

        for r in self.reader:
            # OSError: telling position disabled by next() call so this for now.
            # TODO: It's a waste of CPU to make it back into a string.
            self.approx_position += len(str(r))
            yield Pinnate(data=r)

    @property
    def data(self):
        raise NotImplementedError("TODO")

    @property
    def schema(self):
        raise NotImplementedError("TODO")

    @property
    def progress(self):
        if self.access != AccessMode.READ or self.file_size is None or self.approx_position == 0:
            return None

        return self.approx_position / self.file_size

    def add(self, data):
        """
        Write record to ndjson file.
        @param data: (dict or Pinnate) - must be safe to serialise to JSON so no dates etc.
        """
        if self.access != AccessMode.WRITE:
            raise ValueError("Write attempted on dataset opened in READ mode.")

        self.connect()

        if isinstance(data, dict):
            self.writer.writerow(data)
        elif isinstance(data, Pinnate):
            self.writer.writerow(data.as_dict())
        else:
            raise ValueError("data isn't an accepted type. Only (dict) or (Pinnate) are accepted.")

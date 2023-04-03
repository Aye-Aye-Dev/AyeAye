"""
Created on 17 Dec 2020

@author: si
"""
try:
    import ndjson
except ModuleNotFoundError:
    pass

from ayeaye.connectors.base import AccessMode, FileBasedConnector, FilesystemEnginePatternMixin
from ayeaye.pinnate import Pinnate


class NdjsonConnector(FileBasedConnector, FilesystemEnginePatternMixin):
    engine_type = "ndjson://"
    default_character_encoding = "utf-8-sig"

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
            raise NotImplementedError("READWRITE access not yet implemented")

    def _reset(self):
        FileBasedConnector._reset(self)
        self.reader = None
        self.writer = None
        self.approx_position = 0

    def connect(self):
        if self.reader is None and self.writer is None:
            if self.access == AccessMode.READ:
                FileBasedConnector.connect(self)
                self.reader = ndjson.reader(self._file_handle)

            elif self.access == AccessMode.WRITE:
                FileBasedConnector.connect(self)
                self.writer = ndjson.writer(self._file_handle)

            else:
                raise ValueError("Unknown access mode")

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

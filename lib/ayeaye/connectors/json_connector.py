"""
Created on 15 Apr 2020

@author: si
"""
import json

from ayeaye.connectors.base import AccessMode, FileBasedConnector
from ayeaye.pinnate import Pinnate


class JsonConnector(FileBasedConnector):
    engine_type = "json://"
    optional_engine_url_args = FileBasedConnector.optional_engine_url_args + ["indent"]
    default_character_encoding = "utf-8-sig"

    def __init__(self, *args, **kwargs):
        """
        Single JSON file loaded into memory and made available as a :class:`Pinnate` object.

        For args: @see :class:`connectors.base.DataConnector`

        additional args for JsonConnector
         None

        Connection information-
            engine_url format is
            json://<filesystem absolute path>[;encoding=<character encoding>][;indent=<spaces when pretty printing write output>]
        e.g. json:///data/my_project/the_data.json;encoding=latin-1

        """
        self._reset()
        super().__init__(*args, **kwargs)

    @property
    def engine_params(self):
        if self._engine_params is None:
            self._engine_params = self._build_engine_params()

            for typed_param in ["indent"]:
                if typed_param in self._engine_params:
                    self._engine_params[typed_param] = int(self._engine_params[typed_param])

        return self._engine_params

    def _reset(self):
        FileBasedConnector._reset(self)
        self._doc = None

    def connect(self):
        """
        When in AccessMode.READ, read the contents of the file into a :class:`Pinnate` instance
        that is available as self.data.

        In AccessMode.WRITE mode connect() doesn't do anything because file handles aren't kept
        open by the JsonConnector. The write operation is in :method:`_data_write`.
        """
        if self._doc is None:
            FileBasedConnector.connect(self)

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        raise NotImplementedError("Not an iterative dataset. Use .data instead.")

    def _data_read(self):
        if self._doc is None:
            self.connect()
            as_native = json.load(self._file_handle)
            self._doc = Pinnate(as_native)

            if self.access == AccessMode.READWRITE:
                self._file_handle.seek(0)

        return self._doc

    def _data_write(self, new_data):
        """
        Set the contents of a JSON file. `new_data` can be an instance of :class:`Pinnate` or any
        python datatype that will serialise into JSON.

        Will raise TypeError if the data can't be serialised to JSON.

        @param new_data: (mixed, see description)
        """
        if self.access not in [AccessMode.WRITE, AccessMode.READWRITE]:
            raise ValueError("Write attempted on dataset opened in READ mode.")

        self.connect()

        json_args = {}
        if "indent" in self.engine_params:
            json_args["indent"] = self.engine_params["indent"]

        if isinstance(new_data, Pinnate):
            as_json = json.dumps(new_data.as_dict(), **json_args)
        else:
            as_json = json.dumps(new_data, **json_args)

        # Data is written to beginning of file (it might be readwrite or already written to);
        # write to disk immediately (i.e. flush); @see :method:`connect`.
        self._file_handle.seek(0)
        self._file_handle.write(as_json)
        # truncate rest of the file as the previous contents might have been longer
        self._file_handle.truncate()
        self._file_handle.flush()

    data = property(fget=_data_read, fset=_data_write)

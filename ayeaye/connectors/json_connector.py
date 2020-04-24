'''
Created on 15 Apr 2020

@author: si
'''
import json
import os

from ayeaye.connectors.base import DataConnector, AccessMode
from ayeaye.pinnate import Pinnate


class JsonConnector(DataConnector):
    engine_type = 'json://'

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
        super().__init__(*args, **kwargs)

        self._doc = None
        self._encoding = None
        self._engine_params = None

    @property
    def engine_params(self):
        if self._engine_params is None:
            self._engine_params = self._decode_engine_url(self.engine_url)

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

    def _decode_engine_url(self, engine_url):
        """
        Raises value error if there is anything odd in the URL.

        @param engine_url: (str)
        @return: (Pinnate) with .file_path
                                and optional:
                                    .encoding
                                    .indent - integer number of spaces for pretty printing.
                                            only used in write mode.
        """
        path_plus = engine_url.split(self.engine_type)[1].split(';')
        file_path = path_plus[0]
        d = {'file_path': file_path}
        if len(path_plus) > 1:
            for arg in path_plus[1:]:
                k, v = arg.split("=", 1)
                if k not in ['encoding', 'indent']:
                    raise ValueError(f"Unknown option in JSON: {k}")
                else:
                    if k == 'indent':
                        d[k] = int(v)
                    else:
                        d[k] = v

        return Pinnate(d)

    def close_connection(self):
        self._doc = None

    def connect(self):
        """
        When in AccessMode.READ, read the contents of the file into a :class:`Pinnate` instance
        that is available as self.data.

        In AccessMode.WRITE mode connect() doesn't do anything because file handles aren't kept
        open by the JsonConnector. The write operation is in :method:`_data_write`.
        """
        if self._doc is None:
            file_path = self.engine_url.split(self.engine_type)[1]

            if self.access in [AccessMode.READ, AccessMode.READWRITE]:

                if not (os.path.isfile(file_path) and os.access(file_path, os.R_OK)):

                    if self.access == AccessMode.READ:
                        raise ValueError(f"File '{file_path}' not readable")

                    else:
                        with open(self.engine_params.file_path, 'r', encoding=self.encoding) as f:
                            as_native = json.load(f)
                            self._doc = Pinnate(as_native)

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        raise NotImplementedError("Not an iterative dataset. Use .data instead.")

    def _data_read(self):
        self.connect()
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

        json_args = {}
        if 'indent' in self.engine_params:
            json_args['indent'] = self.engine_params['indent']

        if isinstance(new_data, Pinnate):
            as_json = json.dumps(new_data.as_dict(), **json_args)
        else:
            as_json = json.dumps(new_data, **json_args)

        # Data is written to disk immediately. The file handle isn't left open.
        # @see :method:`connect`.
        with open(self.engine_params.file_path, 'w', encoding=self.encoding) as f:
            f.write(as_json)

    data = property(fget=_data_read, fset=_data_write)

    @property
    def schema(self):
        raise NotImplementedError("TODO")

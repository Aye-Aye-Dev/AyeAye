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
            json://<filesystem absolute path>[;encoding=<character encoding>]
        e.g. json:///data/my_project/the_data.json;encoding=latin-1

        """
        super().__init__(*args, **kwargs)

        self._doc = None
        self._encoding = None
        self._engine_params = None

        if self.access != AccessMode.READ:
            raise NotImplementedError('Write access not yet implemented')

    @property
    def engine_params(self):
        if self._engine_params is None:
            self._engine_params = self._decode_engine_url(self.engine_url)

            if 'encoding' in self._engine_params:
                self._encoding = self.engine_params.encoding

            if 'start' in self._engine_params or 'end' in self._engine_params:
                raise NotImplementedError("TODO")

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
                                and optional: .encoding
        """
        path_plus = engine_url.split(self.engine_type)[1].split(';')
        file_path = path_plus[0]
        d = {'file_path': file_path}
        if len(path_plus) > 1:
            for arg in path_plus[1:]:
                k, v = arg.split("=", 1)
                if k not in ['encoding']:
                    raise ValueError(f"Unknown option in JSON: {k}")
                else:
                    d[k] = v

        return Pinnate(d)

    def close_connection(self):
        self._doc = None

    def connect(self):
        if self._doc is None:
            file_path = self.engine_url.split(self.engine_type)[1]

            if not os.path.isfile(file_path) or not os.access(file_path, os.R_OK):
                raise ValueError(f"File '{file_path}' not readable")

            with open(self.engine_params.file_path, 'r', encoding=self.encoding) as f:
                as_native = json.load(f)
                self._doc = Pinnate(as_native)

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        raise NotImplementedError("Not an iterative dataset. Use .data instead.")

    @property
    def data(self):
        self.connect()
        return self._doc

    @property
    def schema(self):
        raise NotImplementedError("TODO")

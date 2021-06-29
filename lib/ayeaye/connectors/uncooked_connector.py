'''
Created on 19 May 2021

@author: si
'''
import os

from ayeaye.connectors.base import DataConnector, AccessMode


class UncookedConnector(DataConnector):
    engine_type = 'file://'

    def __init__(self, *args, **kwargs):
        """
        Connector to use a raw local file.

        The file will be opened on demand. i.e. reading .data or getting the .file_handle. Getting
        the .file_path will not open the file.

        For args: @see :class:`connectors.base.DataConnector`

        additional args for UncookedConnector
         None

        TODO binary open

        Connection information-
            engine_url format is
            file://<filesystem absolute path>[;encoding=<character encoding>]
        e.g. file:///data/my_project/interesting_notes.txt;encoding=latin-1
        """
        super().__init__(*args, **kwargs)

        self._reset()

        if self.access == AccessMode.READWRITE:
            raise NotImplementedError('READWRITE access not yet implemented')

    def _reset(self):
        self._file_handle = None  # lazy eval, use self.file_handle
        self._encoding = None
        self._engine_params = None
        self.file_size = None

    @property
    def file_handle(self):
        """
        File handle to open file for operations such as :method:`read`()
        """
        self.connect()
        return self._file_handle

    @property
    def file_path(self):
        """
        @return: (str) filesystem path to file
        """
        return self.engine_params.file_path

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
        if self._file_handle is not None:
            self._file_handle.close()
        self._reset()

    def connect(self):
        if self._file_handle is None:

            if self.access == AccessMode.READ:
                self._file_handle = open(self.engine_params.file_path, 'r', encoding=self.encoding)
                self.file_size = os.stat(self.engine_params.file_path).st_size

            elif self.access == AccessMode.WRITE:
                self._file_handle = open(self.engine_params.file_path, 'w', encoding=self.encoding)

            else:
                raise ValueError('Unknown access mode')

    def __len__(self):
        """
        @return: (int) file size
        """
        if self.access != AccessMode.READ:
            raise NotImplementedError("Not yet available in write mode")

        self.connect()
        return self.file_size

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        "Iteration not possible"

        # MAYBE make a readlines method?
        raise TypeError("Can't iterate through a file")

    @property
    def data(self):
        """
        @return: contents of entire file. Type depends on encoding arguments.
        """
        if self.access != AccessMode.READ:
            raise ValueError("Not open in read mode")

        file_content = self.file_handle.read()
        return file_content

    @data.setter
    def data(self, file_content):
        """
        attribute based setter.

        e.g.
        c = UncookedConnector(engine_url="file://mydata_file", access=ayeaye.AccessMode.WRITE)
        c.data = "hello world!"
        """
        if self.access != AccessMode.WRITE:
            raise ValueError("Not open in write mode")

        file_content = self.file_handle.write(file_content)

    @property
    def schema(self):
        raise TypeError("Uncooked connectors don't have rich interfaces like schemas")

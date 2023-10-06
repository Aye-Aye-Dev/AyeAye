"""
Created on 19 May 2021

@author: si
"""
from ayeaye.connectors.base import AccessMode, FileBasedConnector


class UncookedConnector(FileBasedConnector):
    engine_type = "file://"
    optional_args = {
        "file_mode": "t",
    }

    def __init__(self, *args, **kwargs):
        """
        Connector to use a raw local file.

        The file will be opened on demand. i.e. reading .data or getting the .file_handle. Getting
        the .file_path will not open the file.

        For args: @see :class:`connectors.base.DataConnector`

        additional args for UncookedConnector
            file_mode (str) - Either 'b' for binary or 't' (default) text. Other modes not yet
                    supported.
                    Mode to open the file in. See-
                    https://docs.python.org/3/library/functions.html#open

        Connection information-
            engine_url format is
            file://<filesystem absolute path>[;encoding=<character encoding>]
        e.g. file:///data/my_project/interesting_notes.txt;encoding=latin-1
        """
        self._reset()
        super().__init__(*args, **kwargs)

        if self.access == AccessMode.READWRITE:
            raise NotImplementedError("READWRITE access not yet implemented")

        # this overwrites a class variable in :class:`FileBasedConnector`
        if self.file_mode not in ["b", "t"]:
            raise ValueError(f"File mode: {self.file_mode} not supported")

    def _reset(self):
        FileBasedConnector._reset(self)
        self._file_content = None  # used in read mode

    @property
    def file_handle(self):
        """
        File handle to open file for operations such as :meth:`read`()
        """
        self.connect()
        return self._file_handle

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

        if self._file_content is None:
            self._file_content = self.file_handle.read()

        return self._file_content

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

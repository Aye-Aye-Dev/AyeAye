'''
Created on 14 Jan 2020

@author: si
'''
import csv
import os

from ayeaye.connectors.base import DataConnector, AccessMode
from ayeaye.pinnate import Pinnate


class CsvConnector(DataConnector):
    engine_type = 'csv://'

    def __init__(self, *args, **kwargs):
        """
        Connector to Comma Separated Value (CSV) files.

        For args: @see :class:`connectors.base.DataConnector`

        additional args for CsvConnector
         None

        Connection information-
            engine_url format is csv://<filesystem absolute path>data_file.csv[;start <line number>][;end <line number>]
        e.g. csv:///data/my_project/all_the_data.csv
        """
        super().__init__(*args, **kwargs)

        self.delimiter = ','

        self.file_handle = None
        self.csv = None
        self.csv_fields = None # this will change when schemas are implemented
        self.file_size = None
        self.approx_position = 0

        if self.access != AccessMode.READ:
            raise NotImplementedError('Write access not yet implememted')

    def __del__(self):
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None

    def connect(self):
        if self.csv is None:
            file_path = self.engine_url.split(self.engine_type)[1]
            self.file_handle = open(file_path, 'r')
            self.file_size = os.stat(file_path).st_size
            self.csv = csv.DictReader(self.file_handle, delimiter=self.delimiter)
            self.csv_fields = self.csv.fieldnames

    def __len__(self):
        raise NotImplementedError("TODO")


    def __getitem__(self, key):
        raise NotImplementedError("TODO")


    def __iter__(self):
        self.connect()
        for raw in self.csv:
            # OSError: telling position disabled by next() call so this for now
            self.approx_position += len(self.delimiter.join(raw.values()))
            yield Pinnate(data=raw)


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

class TsvConnector(CsvConnector):
    """
    Tab separated values. See :class:`CsvConnector`
    """
    engine_type = 'tsv://'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.delimiter = '\t'

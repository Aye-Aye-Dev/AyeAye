'''
Created on 4 Mar 2020

@author: si
'''
'''
Created on 14 Jan 2020

@author: si
'''
try:
    import pyarrow.parquet as pq
except ModuleNotFoundError:
    pass

import os

from ayeaye.connectors.base import DataConnector, AccessMode
from ayeaye.pinnate import Pinnate


class ParquetConnector(DataConnector):
    engine_type = 'parquet://'

    def __init__(self, *args, **kwargs):
        """
        Connector to Apache Parquet data files and (TODO) datasets.
        See https://parquet.apache.org/

        Uses Apache arrow.
        See https://arrow.apache.org/docs/index.html

        WARNING this module is more experimental than you can possibly imagine.

        For args: @see :class:`connectors.base.DataConnector`

        additional args for ParquetConnector
         None

        Connection information-
            engine_url format is parquet://<filesystem absolute path>data.parquet
        """
        super().__init__(*args, **kwargs)

        self.table = None
        self.row_count = None

        if self.access != AccessMode.READ:
            raise NotImplementedError('Write access not yet implemented')

    def connect(self):
        if self.table is None:

            engine_params = self.ignition._decode_filesystem_engine_url(self.engine_url)
            file_path = engine_params.file_path
            if os.path.isdir(file_path):
                raise NotImplementedError("Parquet datasets are not yet supported")

            if not os.path.isfile(file_path) or not os.access(file_path, os.R_OK):
                raise ValueError(f"File '{file_path}' not readable")

            self.table = pq.read_table(file_path)
            self.row_count = self.table.num_rows

    def close_connection(self):
        self.table = None
        self.row_count = None

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        self.connect()

        if self.row_count is None:
            raise ValueError("Number of rows not loaded from source file")

        self.current_row = 0
        for table_batch in self.table.to_batches():
            big_chunk = table_batch.to_pydict()

            # Check assumptions about chunks and relationship with tables....
            #
            # This is testing the assumption that each column has same number of rows.
            # This probably needs to consider schema as fields presumably could be
            # optional.
            chunk_item_lengths = [len(c) for c in big_chunk.values()]
            chunk_length = chunk_item_lengths[0]
            for a_chunk_length in chunk_item_lengths:
                if a_chunk_length != chunk_length:
                    msg = "Unable to process tables with optional fields."
                    raise NotImplementedError(msg)

            # assumption: table rows could be split between batches
            for current_index in range(chunk_length):
                row_as_dict = {k: big_chunk[k][current_index] for k in big_chunk.keys()}
                yield Pinnate(data=row_as_dict)
                self.current_row += 1

    @property
    def data(self):
        raise NotImplementedError("TODO")

    def as_pandas(self):
        """
        @return: (Pandas dataframe)
        """
        self.connect()
        return self.table.to_pandas()

    @property
    def schema(self):
        raise NotImplementedError("TODO")

    @property
    def progress(self):
        if self.access != AccessMode.READ or self.row_count is None or self.current_row == 0:
            return None

        return self.current_row / self.file_size

'''
Created on 14 Jan 2020

@author: si
'''
import copy
import csv
import os

from ayeaye.connectors.base import DataConnector, AccessMode
from ayeaye.pinnate import Pinnate


class CsvConnector(DataConnector):
    engine_type = 'csv://'
    optional_args = {'field_names': None,
                     'required_fields': None,
                     'expected_fields': None,
                     'alias_fields': None,
                     }

    def __init__(self, *args, **kwargs):
        """
        Connector to Comma Separated Value (CSV) files.

        For args: @see :class:`connectors.base.DataConnector`

        additional args for CsvConnector
            fieldnames (sequence, probably a list (str) - Field names for all rows in file.
                    Using this argument when reading forces the CSV module to treat the first line
                    of the file as data, not as a header. When used in write mode it is the order
                    of fields in the output csv file.

            required_fields (sequence, set or list (str)) - fields must be present in header. Other
                    fields may also exist. Raises ValueError if not.
                    Error to call this in write mode. A ValueError will be raised.

            expected_fields (sequence (str)) - fields must be exactly present in header in the
                    order given. Other fields may NOT also exist. Raises ValueError if not.
                    Error to call this in write mode. A ValueError will be raised.

            alias_fields (sequence of str) or (dict)) - if sequence (i.e. a list) it must be the
                    same length as the actual fields in the header. If a dictionary the key is the
                    original field name and the value is alias for the field.
                    Error to call this in write mode or with 'fieldnames' argument. A ValueError
                    will be raised.

        Connection information-
            engine_url format is
            csv://<filesystem absolute path>data_file.csv[;start=<line number>][;end=<line number>][;encoding=<character encoding>]
        e.g. csv:///data/my_project/all_the_data.csv
        """
        super().__init__(*args, **kwargs)

        # fieldnames are loaded from construction args or from field. This will be unified when
        # schemas are implemented. For now, keep track so loading fieldnames from file doesn't
        # make a :method:`_reset`
        self.base_field_names = copy.copy(self.field_names)

        self.delimiter = ','
        self._reset()

        if self.access == AccessMode.READWRITE:
            raise NotImplementedError('Read+Write access not yet implemented')

    def _reset(self):
        self.file_handle = None
        self.csv = None
        self._encoding = None
        self._engine_params = None
        self.file_size = None
        self.approx_position = 0
        self.field_names = copy.copy(self.base_field_names)

    @property
    def engine_params(self):
        """
        @return: (Pinnate) with .file_path
                        and optional: .encoding .start and .end
        """
        if self._engine_params is None:
            self._engine_params = self.ignition._decode_filesystem_engine_url(
                self.engine_url,
                optional_args=['encoding', 'start', 'end']
            )

            if 'encoding' in self._engine_params:
                self._encoding = self.engine_params.encoding

            for typed_param in ['start', 'end']:
                if typed_param in self.engine_params:
                    self.engine_params[typed_param] = int(self.engine_params[typed_param])

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

    def close_connection(self):
        if self.file_handle is not None:
            self.file_handle.close()
        self._reset()

    def connect(self):
        if self.csv is None:

            if self.access == AccessMode.READ:

                if self.field_names is not None and self.alias_fields is not None:
                    msg = "Can't set field_names and alias them. Just use 'field_names'"
                    raise ValueError(msg)

                if self.field_names is not None:
                    extra_args = {"fieldnames": self.field_names}
                else:
                    extra_args = {}

                self.file_handle = open(self.engine_params.file_path, 'r', encoding=self.encoding)
                self.file_size = os.stat(self.engine_params.file_path).st_size

                self.csv = csv.DictReader(self.file_handle,
                                          delimiter=self.delimiter,
                                          **extra_args,
                                          )
                self.field_names = self.csv.fieldnames


                if self.required_fields is not None:

                    required = set(self.required_fields)
                    field_names = set(self.field_names)

                    if not required.issubset(field_names):
                        missing_fields = ",".join(list(required - field_names))
                        msg = f"Missing required field(s): {missing_fields}"
                        raise ValueError(msg)

                if self.expected_fields is not None and self.expected_fields != self.field_names:
                    msg = "Expected fields does match fields found in file."
                    raise ValueError(msg)

                if self.alias_fields is not None:

                    if isinstance(self.alias_fields, dict):
                        replace_fields = [self.alias_fields.get(f, f) for f in self.csv.fieldnames]
                        self.csv.fieldnames = self.field_names = replace_fields

                    elif not isinstance(self.alias_fields, list) \
                        or len(self.alias_fields) != len(self.csv.fieldnames):
                        msg = ("Alias fields must be a dictionary or list with same number of "
                               "items as fields in the file")
                        raise ValueError(msg)

                    else:
                        self.csv.fieldnames = self.field_names = self.alias_fields

            elif self.access == AccessMode.WRITE:

                if self.required_fields is not None\
                    or self.expected_fields is not None\
                    or self.alias_fields is not None:
                    msg = ("The optional arguments: 'required_fields', 'expected_fields', 'alias_fields' "
                           "can't be used in WRITE mode."
                           )
                    raise ValueError(msg)

                # auto create directory
                file_dir = os.path.dirname(self.engine_params.file_path)
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)

                self.file_handle = open(self.engine_params.file_path,
                                        'w',
                                        newline='\n',
                                        encoding=self.encoding
                                        )
                self.csv = csv.DictWriter(self.file_handle,
                                          delimiter=self.delimiter,
                                          fieldnames=self.field_names,
                                          )
                self.csv.writeheader()

            else:
                raise ValueError('Unknown access mode')

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def __iter__(self):
        self.connect()
        for raw in self.csv:
            # OSError: telling position disabled by next() call so this for now
            # str(x) will slightly over count 'None'. None is given by DictReader when
            # trailing commas are omitted for optional fields at end of row.
            self.approx_position += len(self.delimiter.join([str(x) for x in raw.values()]))
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

    def add(self, data):
        """
        Write line to CSV file.
        @param data: (dict or Pinnate)
        """
        if self.access != AccessMode.WRITE:
            raise ValueError("Write attempted on dataset opened in READ mode.")

        # until schemas are implemented, first row determines fields
        if self.csv is None and self.field_names is None:
            if isinstance(data, dict):
                self.field_names = list(data.keys())
            elif isinstance(data, Pinnate):
                self.field_names = list(data.as_dict().keys())

        self.connect()

        if isinstance(data, dict):
            self.csv.writerow(data)
        elif isinstance(data, Pinnate):
            self.csv.writerow(data.as_dict())
        else:
            raise ValueError("data isn't an accepted type. Only (dict) or (Pinnate) are accepted.")


class TsvConnector(CsvConnector):
    """
    Tab separated values. See :class:`CsvConnector`
    """
    engine_type = 'tsv://'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # see note in CsvConnector
        self.base_field_names = copy.copy(self.field_names)
        self.delimiter = '\t'

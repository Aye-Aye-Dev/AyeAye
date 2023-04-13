"""
Created on 14 Jan 2020

@author: si
"""
import copy
import csv
import os

from ayeaye.connectors.base import AccessMode, FileBasedConnector, FilesystemEnginePatternMixin
from ayeaye.pinnate import Pinnate


class CsvConnector(FileBasedConnector, FilesystemEnginePatternMixin):
    engine_type = "csv://"
    optional_args = {
        "field_names": None,
        "required_fields": None,
        "expected_fields": None,
        "alias_fields": None,
    }
    optional_engine_url_args = FileBasedConnector.optional_engine_url_args + ["start", "end"]
    default_character_encoding = "utf-8-sig"
    write_mode_open_args = {"newline": "\n"}

    def __init__(self, *args, **kwargs):
        """
        Connector to Comma Separated Value (CSV) files.

        For args: @see :class:`connectors.base.FileBasedConnector`

        additional args for CsvConnector
            field_names (sequence, probably a list (str) - Field names for all rows in file.
                    Using this argument when reading forces the CSV module to treat the first line
                    of the file as data, not as a header. When used in write mode it is the order
                    of fields in the output csv file and the :method:`add` can be given a record
                    with additional fields (i.e. not in the `field_names` list) that will be
                    silently ignored and not written to the output file.

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
        self._reset()
        super().__init__(*args, **kwargs)

        # fieldnames are loaded from construction args or from field. This will be unified when
        # schemas are implemented. For now, keep track so loading fieldnames from file doesn't
        # make a :method:`_reset`
        self.base_field_names = copy.copy(self.field_names)
        self.delimiter = ","

        if self.access == AccessMode.READWRITE:
            raise NotImplementedError("Read+Write access not yet implemented")

        self._reset()

    def _reset(self):
        FileBasedConnector._reset(self)
        self.csv = None
        self.approx_position = 0
        if hasattr(self, "base_field_names"):
            self.field_names = copy.copy(self.base_field_names)

    @property
    def engine_params(self):
        """
        @return: (Pinnate) with .file_path
                        and optional: .encoding, .start, .end
        """
        if self._engine_params is None:
            self._engine_params = self._build_engine_params()

            for typed_param in ["start", "end"]:
                if typed_param in self._engine_params:
                    self._engine_params[typed_param] = int(self._engine_params[typed_param])

            if "start" in self._engine_params or "end" in self._engine_params:
                raise NotImplementedError("TODO")

        return self._engine_params

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

                FileBasedConnector.connect(self)

                self.csv = csv.DictReader(
                    self._file_handle,
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
                    diff_s = set(self.expected_fields).symmetric_difference(set(self.field_names))
                    diff = ",".join(diff_s)
                    diff_count = len(diff_s)
                    expected = ",".join(self.expected_fields)
                    actual = ",".join(self.field_names)
                    msg = (
                        f"Expected fields does match fields found in file. There are {diff_count} "
                        f"difference(s): [{diff}] expected: [{expected}] but found: [{actual}]"
                    )
                    raise ValueError(msg)

                if self.alias_fields is not None:
                    if isinstance(self.alias_fields, dict):
                        replace_fields = [self.alias_fields.get(f, f) for f in self.csv.fieldnames]
                        self.csv.fieldnames = self.field_names = replace_fields

                    elif not isinstance(self.alias_fields, list) or len(self.alias_fields) != len(
                        self.csv.fieldnames
                    ):
                        msg = (
                            "Alias fields must be a dictionary or list with same number of "
                            "items as fields in the file"
                        )
                        raise ValueError(msg)

                    else:
                        self.csv.fieldnames = self.field_names = self.alias_fields

            elif self.access == AccessMode.WRITE:
                if (
                    self.required_fields is not None
                    or self.expected_fields is not None
                    or self.alias_fields is not None
                ):
                    msg = (
                        "The optional arguments: 'required_fields', 'expected_fields', 'alias_fields' "
                        "can't be used in WRITE mode."
                    )
                    raise ValueError(msg)

                self.auto_create_directory()

                FileBasedConnector.connect(self)

                self.csv = csv.DictWriter(
                    self._file_handle,
                    delimiter=self.delimiter,
                    fieldnames=self.field_names,
                )
                self.csv.writeheader()

            else:
                raise ValueError("Unsupported access mode")

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
            _d = data
        elif isinstance(data, Pinnate):
            _d = data.as_dict()
        else:
            raise ValueError("data isn't an accepted type. Only (dict) or (Pinnate) are accepted.")

        if self.field_names:
            # the CSV module needs the dictionary passed to write row to match the fieldnames. It's
            # a common scenario to extract just a few fields, these have already been passed to
            # the CsvConnector so just extract the fields needed.
            data_extract = {fn: _d[fn] for fn in self.field_names if fn in _d}
        else:
            data_extract = _d

        self.csv.writerow(data_extract)


class TsvConnector(CsvConnector):
    """
    Tab separated values. See :class:`CsvConnector`
    """

    engine_type = "tsv://"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # see note in CsvConnector
        self.base_field_names = copy.copy(self.field_names)
        self.delimiter = "\t"

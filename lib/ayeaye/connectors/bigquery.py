
try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
except ModuleNotFoundError:
    pass

from ayeaye.connectors.base import AccessMode, DataConnector


class BigQueryConnector(DataConnector):
    engine_type = 'bigquery://'

    optional_args = {'credentials': None, 'auto_schema': False}

    def __init__(self, *args, **kwargs):
        """
        Connector to Google Big Query database.
        Args: @see :class:`connectors.base.DataConnector`

        additional args for BigQueryConnector
         'credentials' : (dict) for access within Google Cloud Platform
         'auto_schema' : (bool) only needed for streaming data (e.g. :method:`add`) and only
                         when table doesn't already exist.

        Connection information-
            engine_url format is bigquery://projectId=<projectId>;datasetId=<datasetId>;[tableId=<table>;]
                    note that table is optional.
                    e.g. bigquery://projectId=my_project;datasetId=nice_food;table=cakes;

            format inspired by jdbc:bigquery://[Host]:[Port];ProjectId=[Project];OAuthType= [AuthValue];\
                                                        [Property1]=[Value1];[Property2]=[Value2];
            but it doesn't specify most of the variables in
            https://cloud.google.com/bigquery/docs/reference/rest/

        The bigquery API is asynchronous by way of jobs. This connector doesn't expose async functionality
        but instead waits for jobs to complete. This is purely to keep use of this connector simple.
        """

        super().__init__(*args, **kwargs)

        # set by :method:`connect`
        self.project_id = self.dataset_id = self.table_id = None
        self.client = None

        # these are loaded and created on demand, use via corresponding methods
        self._dataset = self._table_ref = None

        # other
        self.write_buffer_len = 1000  # optimum value not tested for, just guessed
        self.write_rows_buffer = []
        self.table_connection = None  # different from _table_ref, loaded when needed

    def connect(self):
        if self.client is None:
            self.project_id, self.dataset_id, self.table_id = self._decode_engine_url()
            self.client = bigquery.Client(project=self.project_id, credentials=self.credentials)

    def _decode_engine_url(self):
        """
        Returns:
            project, dataset, table : all are str
        """
        r = dict(projectId=None, datasetId=None, tableId=None)
        for param_section in self.engine_url[len(self.__class__.engine_type):].split(';'):
            if len(param_section) == 0:
                continue
            k, v = param_section.split('=', 1)
            if k in r:
                r[k] = v
        return r['projectId'], r['datasetId'], r['tableId']

    def write_truncate_file(self, ndjson_fh):
        """
        Send the contents of a new line delimited JSON file to a bigquery table.

        Args:
            ndjson_fh (handle in fileIO type object)
        """
        self.connect()
        job = self.client.load_table_from_file(
            file_obj=ndjson_fh,
            destination=self.table_ref,
            job_config=self._get_ndjson_load_job_config(append_mode=False)
        )
        # wait for it to finish loading
        job.result()

    def schema(self):
        raise NotImplementedError("TODO")

    @property
    def table_ref(self):
        """
        This is the table ref.
        https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.table.TableReference.html
        """
        if not self._table_ref:
            self._table_ref = self.dataset.table(self.table_id)
        return self._table_ref

    @property
    def dataset(self):
        if not self._dataset:
            dataset_ref = self.client.dataset(self.dataset_id)
            try:
                dataset = self.client.get_dataset(dataset_ref)
            except NotFound:
                if self.access != AccessMode.WRITE:
                    raise
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "EU"
                dataset = self.client.create_dataset(dataset)
            self._dataset = dataset
        return self._dataset

    def _get_ndjson_load_job_config(self, append_mode=True):
        """
        An instance of this class might create many jobs.

        Args:
            append_mode (bool) if False truncate the table instead
        """
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

        if append_mode:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        job_config.ignore_unknown_values = True
        job_config.max_bad_records = 1000000

        return job_config

    @property
    def data(self):
        """
        All the data. Use :method:`query`.
        TODO slices
        """
        self.connect()
        full_qual_table = f'{self.project_id}.{self.dataset_id}.{self.table_id}'
        yield from self.client.list_rows(full_qual_table)

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def add(self, row):
        """
        Add a row to the connected table.
        This method doesn't write to BQ immediately. call :method:`flush_writes` when
        needed and after last row has been written.
        Args:
            row (dict) or tuple with correct column mappings
        """
        self.write_rows_buffer.append(row)
        if len(self.write_rows_buffer) >= self.write_buffer_len:
            self.flush_writes()

    def flush_writes(self):
        """
        Write any buffered data to BigQuery.

        Note that streaming data is treated slightly differently than file loads.
        See
        https://cloud.google.com/blog/products/gcp/life-of-a-bigquery-streaming-insert

        TLDR;
        The streamed data is committed to column stores later. See the 'Streaming buffer
        statistics' section of the table's details section.
        These data does show up in queries but not preview.
        """
        if len(self.write_rows_buffer) == 0:
            # nothing to do
            return

        self.connect()

        if self.table_connection is None:
            # create when needed

            try:
                self.table_connection = self.client.get_table(self.table_ref)
            except NotFound:
                if not self.auto_schema:
                    msg = "Can't write data as table doesn't exist and auto_schema not set."
                    raise ValueError(msg)

                auto_schema = self._auto_schema(self.write_rows_buffer)
                table = bigquery.Table(self.table_ref, schema=auto_schema)
                self.table_connection = self.client.create_table(table)

        errors = self.client.insert_rows(self.table_connection, self.write_rows_buffer)
        # TODO log the errors
        self.write_rows_buffer = []

    def _auto_schema(self, sample_data):
        """
        Returns:
            big query schema (list of bigquery.SchemaField)
        """
        # super simple for now!
        sample_row = sample_data[0]
        schema = []
        for k, v in sample_row.items():
            field_type = 'STRING'
            if isinstance(v, int):
                field_type = 'INTEGER'
            schema.append(bigquery.SchemaField(k, field_type, mode='REQUIRED'))

        return schema

    def query(self, **kwargs):
        """
        get a subset of :method:`data`.

        Allowed options-
        sql : (str) SQL statement.
        sql_params : (list of tuples) of form
                e.g. [("corpus", "STRING", "romeoandjuliet"),
                      ("min_word_count", "INT64", 250),
                      ]
                      Variable types are those passed to bigquery.ScalarQueryParameter.
                      See https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.query.ScalarQueryParameter.html

                      example
                      sql = 'SELECT * FROM `bbc-datalab.ayeaye_test.my_data` WHERE id > @min_id'
                      sql_params = [("min_id", "INT64", 1234),]

        At present, `sql` arg is mandatory but wont be once we have alternative access patterns.
        """
        assert self.access == AccessMode.READ or AccessMode.READ in self.access

        sql = kwargs.pop('sql', None)
        sql_params = kwargs.pop('sql_params', None)

        if not sql:
            raise NotImplementedError("Only 'sql' has been implemented.")

        self.connect()
        job_config = bigquery.QueryJobConfig()
        if sql_params:
            job_config.query_parameters = [bigquery.ScalarQueryParameter(*sp) for sp in sql_params]
        query_job = self.client.query(sql, job_config=job_config)
        yield from query_job

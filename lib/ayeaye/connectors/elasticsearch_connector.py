try:
    # pipenv install elasticsearch
    from elasticsearch import Elasticsearch
except ModuleNotFoundError:
    pass

from ayeaye.connectors.base import DataConnector, AccessMode


class ElasticsearchConnector(DataConnector):
    engine_type = 'elasticsearch://'

    def __init__(self, *args, **kwargs):
        """
        Connector to Elasticsearch - https://www.elastic.co/elasticsearch/
        Args: @see :class:`connectors.base.DataConnector`

        This connector is very much a beta. Limitations and not fully thought through.
        e.g.
         - just one node

        additional args for ElasticsearchConnector
         None

        Connection information-
            engine_url format is elasticsearch://es_server[:port]/[<default_index>]
            e.g.
                elasticsearch://localhost/my_documents
        """
        super().__init__(*args, **kwargs)

        # set by :method:`connect`
        self.host = self.port = self.default_index = None
        self.client = None

    def close_connection(self):
        self.client = None

    def connect(self):
        if self.client is None:
            self.host, self.port, self.default_index = self._decode_engine_url()
            es_node_args = {'host': self.host}
            if self.port:
                es_node_args['port'] = self.port
            self.client = Elasticsearch([es_node_args])

    def _decode_engine_url(self):
        """
        Returns:
            host, port, default_index
            host, default_index are (str)
            port is (int)
        """
        r = dict(host=None, port=None, index=None)
        s_url = self.engine_url[len(self.__class__.engine_type):]

        if '/' in s_url:
            host_port, r['index'] = s_url.split('/', 1)
            if r['index'] == '':
                r['index'] = None
        else:
            host_port = s_url

        if ':' in host_port:
            r['host'], r['port'] = host_port.split(':', 1)
            r['port'] = int(r['port'])
        else:
            r['host'] = host_port

        return r['host'], r['port'], r['index']

    def schema(self):
        raise NotImplementedError("TODO")

    def __len__(self):
        raise NotImplementedError("TODO")

    def __getitem__(self, key):
        raise NotImplementedError("TODO")

    def add(self, document, doc_id=None, document_type=None, index=None):
        """
        Write document to index.

        @param document: (mixed) must be serialisable to JSON
        @param doc_id: document unique identifier. Will overwrite existing docs. with same id.
        @param document_type: (str)
        @param index: (str) must be set in engine_url or here
        @return: (dict) from ES library
        """
        # TODO expand data to include binary and instance of :class:`Pinnate` but needs a way of
        # de-serialising on retrieve.

        if self.access != AccessMode.WRITE and self.access != AccessMode.READWRITE:
            raise ValueError("Write attempted on dataset opened in READ mode.")

        self.connect()

        resolved_index = index or self.default_index
        if not resolved_index:
            raise ValueError("Unknown index: must be set in engine_url or as argument")

        # index(index, body, doc_type=None, id=None, params=None, headers=None)
        w = self.client.index(index=resolved_index,
                              doc_type=document_type,
                              id=doc_id,
                              body=document
                              )
        return w

    def fetch(self, doc_id=None, document_type=None, index=None):
        """
        @see :method:`add` but without `document` argument
        @return: the orginal document.
        """
        if self.access != AccessMode.READ and self.access != AccessMode.READWRITE:
            raise ValueError("Read attempted on dataset opened in WRITE mode.")

        self.connect()

        resolved_index = index or self.default_index
        if not resolved_index:
            raise ValueError("Unknown index: must be set in engine_url or as argument")

        # get(index, id, doc_type=None, params=None, headers=None)
        r = self.client.get(resolved_index, doc_id, doc_type=document_type)
        return r['_source']

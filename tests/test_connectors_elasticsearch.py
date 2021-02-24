import unittest

import ayeaye
from ayeaye.connectors.elasticsearch_connector import ElasticsearchConnector


class TestElasticsearchConnector(unittest.TestCase):

    def test_engine_decode(self):

        samples = [('localhost', None, None, "elasticsearch://localhost/"),
                   ('127.0.0.1', None, None, "elasticsearch://127.0.0.1"),
                   ('es_server', 9200, 'my_docs', "elasticsearch://es_server:9200/my_docs"),
                   ]
        for expected_host, expected_port, expected_index, engine_url in samples:
            c = ElasticsearchConnector(engine_url=engine_url)
            host, port, default_index = c._decode_engine_url()
            self.assertEqual(expected_host, host)
            self.assertEqual(expected_port, port)
            self.assertEqual(expected_index, default_index)

    @unittest.skip("Needs integration test suite")
    def test_read_write_document(self):
        "Happy ES paths"

        c = ElasticsearchConnector(engine_url="elasticsearch://localhost/my_docs_xyz",
                                   access=ayeaye.AccessMode.READWRITE
                                   )
        c.add({'hello': 'world'})
        # TODO - search and find a document without an id

        c.add({'hello': 'there'}, doc_id=123)
        result = c.fetch(doc_id=123)
        self.assertEqual({'hello': 'there'}, result, 'Without index or document type should match')

        c.add({'hello': 'Bob'}, doc_id='124', document_type='hr_records', index='my_docs_abc')
        result = c.fetch(doc_id='124', document_type='hr_records', index='my_docs_abc')
        self.assertEqual({'hello': 'Bob'}, result, 'With index and document type should match')

    @unittest.skip("Needs integration test suite")
    def test_missing_index(self):

        c = ElasticsearchConnector(engine_url="elasticsearch://localhost/",
                                   access=ayeaye.AccessMode.WRITE
                                   )
        with self.assertRaises(ValueError) as context:
            c.add({'hello': 'there'}, doc_id=123)
        self.assertIn("Unknown index", str(context.exception))

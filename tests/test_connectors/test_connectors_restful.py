import unittest

import responses

import ayeaye
from ayeaye.connectors import RestfulConnector, RestfulConnectorConnectionError


class TestRestfulConnector(unittest.TestCase):
    @responses.activate
    def test_get(self):
        "http GET a document"

        responses.add(
            responses.GET,
            "http://zooological-online.mock/parrots/african_grey/?doc_format=short",
            json={"scientific_name": "Spoonius Pidgeonus"},
            status=200,
        )

        c = RestfulConnector(engine_url="http://zooological-online.mock")

        url_params = ayeaye.Pinnate({"doc_format": "short"})
        species_facts = c.get("/parrots/african_grey/", params=url_params)

        self.assertEqual(200, c.last_http_status)

        msg = "JSON doc. accessed as a Pinnate (attrib based) obj."
        self.assertEqual(species_facts.scientific_name, "Spoonius Pidgeonus", msg)

        # check statistics are recorded
        self.assertEqual(1, c.statistics.requests_count)

        msg = "Some time was spent waiting for the server"
        self.assertGreater(c.statistics.requests_total_time, 0, msg)
        self.assertGreater(c.statistics.requests_slowest_time, 0, msg)

        msg = "Only one request so it's the slowest. Note url is malformed. Known missing feature."
        self.assertEqual(
            "http://zooological-online.mock/parrots/african_grey/{'doc_format': 'short'}",
            c.statistics.requests_slowest_url,
            msg,
        )

    @responses.activate
    def test_post(self):
        "http POST a new document"

        responses.add(
            responses.POST,
            "http://zooological-online.mock/parrots/",
            status=201,  # doc added successfully
        )

        c = RestfulConnector(
            engine_url="http://zooological-online.mock", access=ayeaye.AccessMode.WRITE
        )

        new_parrot = ayeaye.Pinnate({"common_name": "Blue-and-yellow macaw"})
        c.post("/parrots/", new_parrot)

        self.assertEqual(201, c.last_http_status)

    # @responses.activate
    def test_post_host_down(self):
        "Can't connect to host"

        c = RestfulConnector(
            engine_url="http://zooological-online.mock", access=ayeaye.AccessMode.WRITE
        )

        new_parrot = ayeaye.Pinnate({"common_name": "Blue-and-yellow macaw"})
        with self.assertRaises(RestfulConnectorConnectionError):
            c.post("/parrots/", new_parrot)

    @responses.activate
    def test_patch(self):
        "http PATCH an existing document"

        responses.add(
            responses.PATCH,
            "http://zooological-online.mock/parrots/african_grey/",
            status=200,  # could be a 204
        )

        c = RestfulConnector(
            engine_url="http://zooological-online.mock", access=ayeaye.AccessMode.READWRITE
        )

        updated_parrot = {"alias": "Congo grey parrot"}
        c.patch("/parrots/african_grey/", updated_parrot)

        self.assertEqual(200, c.last_http_status)

    @responses.activate
    def test_delete(self):
        "http DELETE an existing document"

        responses.add(
            responses.DELETE,
            "http://zooological-online.mock/parrots/african_grey/",
            status=200,  # could also be a 204 (No Content) or 202 (Accepted)
        )

        c = RestfulConnector(
            engine_url="http://zooological-online.mock", access=ayeaye.AccessMode.READWRITE
        )

        c.delete("/parrots/african_grey/")

        self.assertEqual(200, c.last_http_status)

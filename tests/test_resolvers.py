import unittest

from ayeaye.connect_resolve import connector_resolver
from ayeaye.connect import Connect
from ayeaye.connectors.csv_connector import CsvConnector
from ayeaye.model import Model


class FakeModel:
    insects = Connect(engine_url="fake://bugsDB")

    def __init__(self):
        self._connections = {}


class TestResolve(unittest.TestCase):
    """
    Runtime variables made available through ayeaye.connect_resolve.connector_resolver.
    """

    def test_multi_connector_resolve(self):
        """
        MultiConnector + ConnectorResolver.
        Other tests for this in :class:`TestConnectors`.
        """

        def simple_resolver(unresolved_engine_url):
            return unresolved_engine_url.format(**{'data_version': '1234'})

        # A MultiConnector
        c = Connect(engine_url=["csv://my_path_x/data_{data_version}.csv",
                                "csv://my_path_y/data_{data_version}.csv"
                                ]
                    )

        with connector_resolver.context(simple_resolver):
            resolved_engine_urls = [data_conn.engine_url for data_conn in c]

        expected_urls = ['csv://my_path_x/data_1234.csv', 'csv://my_path_y/data_1234.csv']
        self.assertEqual(expected_urls, resolved_engine_urls)

    def test_multi_connector_add(self):
        """
        Use MultiConnector's convenience method for adding engine_urls at run time.
        Also ensure the connector resolver is still being used.
        """
        class FishStocksCollator(FakeModel):
            fish = Connect(engine_url=['csv://{file_location}/pond_1.csv',
                                       'csv://{file_location}/pond_2.csv',
                                       ]
                           )

            def build(self):
                # add a new dataset at runtime
                c = self.fish.add_engine_url('csv://{file_location}/pond_3.csv')
                assert isinstance(c, CsvConnector)
                assert c.engine_url == 'csv:///data/pond_3.csv'

        def file_location_resolver(unresolved_engine_url):
            return unresolved_engine_url.format(**{'file_location': '/data'})

        with connector_resolver.context(file_location_resolver):
            m = FishStocksCollator()
            m.build()
            all_urls = [connector.engine_url for connector in m.fish]

        expected_urls = ['csv:///data/pond_1.csv', 'csv:///data/pond_2.csv',
                         'csv:///data/pond_3.csv',
                         ]
        self.assertEqual(expected_urls, all_urls)

    def test_resolve_engine_url(self):
        """
        The engine_url contains a parameter that is replaced on demand.
        """
        msg = "There are existing resolver callables before the test has started"
        self.assertEqual(0, len(connector_resolver.unnamed_callables), msg)

        class MockFakeEngineResolver:
            "Record when it's used and just substitute {data_version} with '1234'"

            def __init__(self):
                self.has_been_called = False

            def __call__(self, unresolved_engine_url):
                self.has_been_called = True
                return unresolved_engine_url.format(**{'data_version': '1234'})

        c = CsvConnector(engine_url="csv://my_path/data_{data_version}.csv")

        m_resolver = MockFakeEngineResolver()
        with connector_resolver.context(m_resolver):
            self.assertFalse(m_resolver.has_been_called, "Should only be called on demand")
            msg = "One resolver exists during the .context"
            self.assertEqual(1, len(connector_resolver.unnamed_callables), msg)

            self.assertEqual('csv://my_path/data_1234.csv', c.engine_url)

            msg = "Should have been called after engine_url is available"
            self.assertTrue(m_resolver.has_been_called, msg)

        msg = "At end of with .context the MockFakeEngineResolver should have been removed"
        self.assertEqual(0, len(connector_resolver.unnamed_callables), msg)

    def test_attribute_access_to_instances(self):

        class SaladResolver:
            def available_today(self):
                return ["csv://cucumbers.csv", "csv://cress.csv"]

        fresh_salad = SaladResolver()
        with connector_resolver.context(salad=fresh_salad):
            todays_engine_urls = connector_resolver.salad.available_today()

        self.assertEqual(["csv://cucumbers.csv", "csv://cress.csv"], todays_engine_urls)
        self.assertNotIn('salad', connector_resolver._attr, "Post context clean up failed")

    def test_deferred_attribute_access(self):
        """
        If a Connect uses a callable to return engine_urls at runtime and this callable uses
        connector_resolver's named attributes there is a catch 22. -- the resolver needs the
        attribute to be set before the model class is imported. Solution is a deferred call that
        is only evaluated by Connect._prepare_connection
        """
        class InsectSurvey(Model):
            ants = Connect(engine_url=connector_resolver.my_ants.all_the_files(ant_types="red"))

            def build(self):
                assert self.ants.engine_url == "csv://red_ants.csv"

        # ------- at this point ------------
        # without the deferred call this would have failed by here because `importing` InsectSurvey
        # would have evaluated 'ants = Connect(...)'

        class MyFileResolver:
            def all_the_files(self, ant_types):
                if ant_types == "red":
                    return "csv://red_ants.csv"
                raise ValueError("Should be unreachable in this test")

        files_at_runtime = MyFileResolver()
        with connector_resolver.context(my_ants=files_at_runtime):
            m = InsectSurvey()
            m.build()

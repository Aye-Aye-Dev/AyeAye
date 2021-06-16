import unittest

from ayeaye.connect_resolve import ConnectorResolver, connector_resolver
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
                raise ValueError("This line should be unreachable in this test")

        files_at_runtime = MyFileResolver()
        with connector_resolver.context(my_ants=files_at_runtime):
            m = InsectSurvey()
            m.build()

    def test_without_with_statement(self):
        """
        In unit tests it's helpful to use the same resolver context across a few methods. For
        example in unittest's setUp, tearDown and the test itself.
        """
        class LizardLocator(FakeModel):
            habitats = Connect(engine_url='csv://{file_location}/habitat.csv')

            def get_the_important_engine_url(self):
                return self.habitats.engine_url

        def file_location_resolver(unresolved_engine_url):
            return unresolved_engine_url.format(**{'file_location': '/data'})

        m = LizardLocator()
        with self.assertRaises(ValueError) as exception_context:
            m.get_the_important_engine_url()

        exception_message = str(exception_context.exception)
        msg = "Without a connector_resolver it shouldn't be possible to get the engine_url"
        self.assertIn("Couldn't fully resolve engine URL", exception_message, msg)
        self.assertIn("Missing template variables are: {file_location}", exception_message)

        # using .start() and .finish() instead of a with statement
        local_context = connector_resolver.context(file_location_resolver)
        local_context.start()

        m = LizardLocator()
        self.assertEqual('csv:///data/habitat.csv', m.get_the_important_engine_url())

        msg = "One resolver exists between .start() and .finish()"
        self.assertEqual(1, len(connector_resolver.unnamed_callables), msg)

        # drop the local context
        local_context.finish()

        self.assertEqual(0, len(connector_resolver.unnamed_callables), msg)

    def test_multiple_unnamed_resolvers(self):
        """
        Using ConnectorResolver directly (no as the global/singleton) does chaining unnamed
        resolvers work?
        """
        class IgnoreMissingDict(dict):
            def __missing__(self, key):
                return '{' + key + '}'

        def a2x(e):
            return e.format_map(IgnoreMissingDict(**{'a': 'x'}))

        def b2y(e):
            return e.format_map(IgnoreMissingDict(**{'b': 'y'}))

        def c2z(e):
            return e.format_map(IgnoreMissingDict(**{'c': 'z'}))

        cr = ConnectorResolver()
        cr.add(a2x, b2y, c2z)
        engine_url = cr.resolve("{a}{b}{c}")
        expected_url = "xyz"
        self.assertEqual(expected_url, engine_url)

    def test_named_variables(self):

        with connector_resolver.context(env_secret_password="supersecret"):
            x = Connect(engine_url="mysql://root:{env_secret_password}@localhost/my_database")
            x.connect_standalone()
            self.assertEqual('mysql://root:supersecret@localhost/my_database', x.engine_url)

    def test_deferred_results_not_held(self):
        """
        Regression test for fix. The results of a callable for engine_url were being persisted on
        relayed_kwargs in :method:`Connect._prepare_connection`. DeferredResolution was being used
        in this case and it's a common pattern.
        """
        class AnimalsSurvey(Model):
            rodents = Connect(
                engine_url=connector_resolver.my_survey.sample_data(rodent_type="mice"))

        class ResolverA:
            def sample_data(self, rodent_type):
                if rodent_type == "mice":
                    return "csv://mice_sample_a.csv"
                raise ValueError("This line should be unreachable in this test")

        files_at_runtime = ResolverA()
        with connector_resolver.context(my_survey=files_at_runtime):
            m = AnimalsSurvey()
            first_call_engine_url = m.rodents.engine_url

        class ResolverB:
            def sample_data(self, rodent_type):
                if rodent_type == "mice":
                    return "csv://mice_sample_b.csv"
                raise ValueError("This line should be unreachable in this test")

        files_at_runtime = ResolverB()
        with connector_resolver.context(my_survey=files_at_runtime):
            m = AnimalsSurvey()
            second_call_engine_url = m.rodents.engine_url

        self.assertNotEqual(first_call_engine_url, second_call_engine_url)

    def test_reset_connect_resolve(self):
        """
        :method:`brutal_reset` can be used to stop a failed unit test effecting another unit test.
        """
        connector_resolver.add(x='y')
        with self.assertRaises(ValueError) as exception_context:
            connector_resolver.add(x='z')

        self.assertIn("Attempted to set existing attribute: x", str(exception_context.exception))

        connector_resolver.brutal_reset()
        connector_resolver.add(x='z')

        # don't leave state for other tests
        connector_resolver.brutal_reset()

    @unittest.skip("Callable kwargs not implemented yet")
    def test_callable_mapper_value(self):

        class CheeseSales(Model):
            products = Connect(engine_url="csv://my_path_x/data_{data_version}.csv")

        def simple_resolver(*args):
            return "deep_fried_brie"

        with connector_resolver.context(data_version=simple_resolver):
            m = CheeseSales()
            resolved_engine_url = m.products.engine_url

        self.assertEqual('csv://my_path_x/data_deep_fried_brie.csv', resolved_engine_url)

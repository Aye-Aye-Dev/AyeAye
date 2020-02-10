import unittest

from ayeaye import AccessMode
from ayeaye.connect import Connect


class FakeModel:
    insects = Connect(engine_url="fake://bugsDB")

    def __init__(self):
        self._connections = {}

class TestConnect(unittest.TestCase):

    def test_connect_standalone(self):
        """
        :class:`ayeaye.Connect` can be used outside of the ETL so data discovery can use the same
        way of working as full :class:`ayeaye.Model`s.
        """
        # happy path
        # it works without Connect being part of a ayeaye.Model
        c = Connect(engine_url="fake://MyDataset")
        assert c.data[0] == {'fake': 'data'}

    def test_connect_spare_kwargs(self):
        """
        subclasses of :class:`ayeaye.connectors.base.DataConnector` can be given specific/custom
        kwargs. An exception should be raised when unclaimed spare kwargs remain. This will make
        it harder for users to make mistakes and typos referring to arguments that never come
        into play.
        """
        c = Connect(engine_url="fake://foo", doesntexist='oh dear')
        with self.assertRaises(ValueError):
            # the kwargs are not used until an engine_url is needed
            c._prepare_connection()
    
    def test_connect_within_instantiated_class(self):
        """
        Connect used as a class variable. The parent class, which in practice will be a
        :class:`ayeaye.Model`.
        When used as a class variable in an instantiated class, Connect() will store information
        about the dataset within the parent (i.e. Model) class.
        """
        e0 = FakeModel()
        assert len(e0._connections) == 0
    
        # connect on demand/access
        assert e0.insects is not None
        assert len(e0._connections) == 1
    
    def test_connect_within_class(self):
        """
        Connect used as a class variable. On access it returns a new instance that is separated,
        i.e. not the same object as, the original.
        """
        copy_0 = FakeModel.insects
        copy_1 = FakeModel.insects
        
        assert id(copy_0) != id(copy_1)
    
    def test_custom_kwargs_are_passed(self):
        """
        ayeaye.Connect should relay kwargs to subclasses of DataConnecter
        """
        # using bigquery because it has custom 'credentials' kwarg
        engine_url = 'bigquery://projectId=my_project;datasetId=nice_food;tableId=cakes;'
        c = Connect(engine_url=engine_url, credentials="hello_world")
        # on demand connection
        assert c.data is not None
        assert c._local_dataset.credentials == "hello_world"

    def test_overlay_args(self):
        """
        Make an access=AccessMode.READ connection in a model into access=AccessMode.WRITE.
        The engine_url stays the same.
        """
        class FakeModelWrite:
            insects = FakeModel.insects(access=AccessMode.WRITE)

            def __init__(self):
                self._connections = {}

        f = FakeModelWrite()
        self.assertEqual('fake://bugsDB', f.insects.engine_url)
        self.assertEqual(AccessMode.WRITE, f.insects.access)

    def test_replace_existing_connect(self):

        m = FakeModel()
        with self.assertRaises(ValueError) as context:
            m.insects = "this is a string, not an instance of Connect"
        self.assertEqual("Only Connect instances can be set", str(context.exception))

        self.assertEqual({}, m._connections, "Connections not initialised prior to access")
        self.assertEqual("fake://bugsDB", m.insects.engine_url, "Original connection")

        m.insects = Connect(engine_url="fake://creepyCrawliesDB")
        self.assertEqual("fake://creepyCrawliesDB", m.insects.engine_url, "New connection")

    def test_connect_update(self):
        """
        Take a connection from a model, make a small tweak and set it back into the model.
        Note is isn't a class tweak (that is tested elsewhere), it's on instances.
        """
        m = FakeModel()
        self.assertTrue(AccessMode.READ == m.insects.access, "Expected starting state not found")

        connect = m.insects.connect_instance
        connect_refs = [k for k in m._connections.keys()]

        # change something, this could have been more dramatic, the engine type for example
        connect(access=AccessMode.WRITE)

        # this set will re-prepare the connection
        m.insects = connect

        self.assertTrue(AccessMode.WRITE == m.insects.access, "Change to connection went missing")
        connect_refs_now = [k for k in m._connections.keys()]
        self.assertEqual(connect_refs, connect_refs_now, "Connect instances shouldn't change")

from datetime import datetime

from ayeaye.connectors.fake import FakeDataConnector

class Model:
    """
    Do the thing!
    
    Abstract class 
    
    The thing is probably an ETL (Extract, Transform and Load) task which is at the minimum
    the :method:`build`. It could optionally also have a :method:`pre_build_check`, which is run
    first and must succeed. After the :method:`build` an optional :method:`post_build_check`
    can be used to see if build worked. 
    """
    def __init__(self):
        self._connections = {}

        self.log_to_stdout = True
        self.external_logger = None

    def go(self):
        """
        Run the model.
        """
        self.build()

    def build(self):
        raise NotImplementedError()

    def datasets(self):
        """
        :returns (dict) of dataset connections for this model.
                key is class variable name
                value is :class:`ayeaye.Connect`
        """
        # find :class:`ayeaye.Connect` connections to datasets
        connections = {}
        for obj_name in dir(self):
            obj = getattr(self, obj_name)
            if isinstance(obj, FakeDataConnector):
                connections[obj_name] = obj

        return connections

    def set_logger(self, logger):
        """
        Also log to something with a :method:`write`.
        e.g. StringIO
        """
        self.external_logger = logger

    def log(self, msg, level="INFO"):
        if not (self.log_to_stdout or self.external_logger is not None):
            return

        date_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        msg = "{} {}{}".format(date_str, level.ljust(10), msg)

        if self.external_logger is not None:
            self.external_logger.write(msg)

        if self.log_to_stdout:
            print(msg)

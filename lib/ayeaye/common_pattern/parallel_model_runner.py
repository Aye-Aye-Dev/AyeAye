import ayeaye


class AbstractModelRunner(ayeaye.PartitionedModel):
    """
    Use a `PartitionedModel` to run some normal :class:`ayeaye.Model`s.

    This is overly simplistic, see the Fossa project (https://github.com/Aye-Aye-Dev/fossa) for how
    is should be done in more complex scenarios.

    This class must be subclassed and given a list of models to run. The models mustn't be
    PartitionedModels (as this could create a resource log jam) and the models mustn't depend on each
    other as this isn't checked.

    The models in the list are started in order but they are run in parallel (with the number of
    parallel tasks depending on the number of CPUs) so any dependencies might not have been satisfied.

    @see :method:`tests.test_model_partitioned.TestPartitionedModel.test_parallel_models` for an
    example of this running.
    """

    # subclass must implement this. It's a list of tuples (model_cls, kwargs) where kwargs is a
    # dictionary.
    models = None

    def partition_plea(self):
        m_count = len(self.models)
        p = ayeaye.PartitionedModel.PartitionOption(minimum=1, maximum=m_count, optimal=m_count)
        return p

    def partition_slice(self, slice_count):
        target_method = "run_etl_model"  # this is the method subtasks will run
        subtasks = [(target_method, {"model_position": p}) for p in range(len(self.models))]
        return subtasks

    def run_etl_model(self, model_position):
        """
        Run one of the target models in a separate process. Re-direct it's log messages to
        `self.log`.

        @param model_position: (int)
            `self.models` is a list, run the model in this position.
        """

        class LoopBackLogger:
            """
            Redirect log messages from the target model to ModelRunner's log method.
            """

            def __init__(self, log_prefix, log_target):
                self.log_prefix = log_prefix
                self.log_target = log_target

            def write(self, msg):
                self.log_target.log(f"{self.log_prefix} {msg}")

        model_cls, model_kwargs = self.models[model_position]
        model_name = model_cls.__name__

        external_log = LoopBackLogger(log_prefix=model_name, log_target=self)

        self.log(f"Running model {model_name} from position: {model_position}")
        m = model_cls(**model_kwargs)

        m.set_logger(external_log)
        m.log_to_stdout = False  # avoid duplicate messages

        m.go()

    def build(self):
        self.log("Running ModelRunner")


class A(ayeaye.Model):
    def build(self):
        from_context = ayeaye.connector_resolver.resolve("From the build context: {greeting}")
        self.log(f"This is Model A with context: {from_context}")


class B(ayeaye.Model):
    def __init__(self, init_arg, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.init_arg = init_arg

    def build(self):
        self.log(f"This is Model B with init arg: {self.init_arg}")


class C(ayeaye.Model):
    def build(self):
        self.log("This is Model C")


class ExampleModelRunner(AbstractModelRunner):
    models = [
        (A, {}),
        (B, {"init_arg": "hi model B"}),
        (C, {}),
    ]


if __name__ == "__main__":
    # run it from the command line like this-
    # $ pipenv shell
    # $ python examples/parallel_model_runner/model_runner.py

    build_context = {"greeting": "Hello command line model!"}
    with ayeaye.connector_resolver.context(**build_context):
        m = ExampleModelRunner()
        m.go()

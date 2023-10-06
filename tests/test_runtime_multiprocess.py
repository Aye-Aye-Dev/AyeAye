import unittest

import ayeaye
from ayeaye.runtime.multiprocess import ProcessPool


class ExamineResolverContext(ayeaye.PartitionedModel):
    def fake_subtask(self):
        try:
            ayeaye.connector_resolver.resolve("{build_environment_variable}")
            build_environment_variable_set = True
        except ValueError:
            build_environment_variable_set = False

        try:
            ayeaye.connector_resolver.resolve("{local_variable}")
            local_variable_set = True
        except ValueError:
            local_variable_set = False

        r = dict(
            build_environment_variable_set=build_environment_variable_set,
            local_variable_set=local_variable_set,
        )
        return r


class TestRuntimeMultiprocess(unittest.TestCase):
    """
    Test the local execution of :class:`ayeaye.Model`s with multiple processes.
    """

    def test_resolver_context_not_inherited(self):
        """
        The resolver context shouldn't be copied to worker processes from the parent. It should be
        explicitly passed in :meth:`ayeaye.runtime.multiprocess.ProcessPool.run_model`.

        In practice they will be the same thing. This behaviour is checked as there was an issue
        with OSX fork() and Linux fork() having differences in memory from parent process? i.e.
        OSX didn't have the context and Linux did.
        """
        workers_count = 1

        with ayeaye.connector_resolver.context(build_environment_variable="is set"):
            msg = "Normal Aye-aye context resolver just sees vars in ayeaye.connector_resolver singleton"
            m = ExamineResolverContext()
            results = m.fake_subtask()
            self.assertTrue(results["build_environment_variable_set"], msg)
            self.assertFalse(results["local_variable_set"], msg)

            msg = "build_environment_variable should be discarded, local_variable should be present"
            proc_pool = ProcessPool(
                processes=workers_count,
                context_kwargs={
                    "mapper": {"local_variable": "is_set"},
                },
            )
            for (
                msg_type,
                method_name,
                method_kwargs,
                subtask_return_value,
            ) in proc_pool.run_subtasks(
                model_cls=ExamineResolverContext, tasks=[("fake_subtask", None)], initialise=None
            ):
                # there will be only one sub-task return - this isn't tested
                if str(msg_type) == "MessageType.COMPLETE":
                    # only checking the final/complete message type
                    self.assertEqual(method_name, "fake_subtask")
                    self.assertEqual(method_kwargs, {})
                    self.assertFalse(subtask_return_value["build_environment_variable_set"], msg)
                    self.assertTrue(subtask_return_value["local_variable_set"], msg)

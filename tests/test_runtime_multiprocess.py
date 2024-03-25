import unittest

import ayeaye
from ayeaye.runtime.multiprocess import LocalProcessPool
from ayeaye.runtime.task_message import (
    task_message_factory,
    TaskComplete,
    TaskLogMessage,
    TaskPartition,
)


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
            proc_pool = LocalProcessPool(max_processes=workers_count)

            subtask_kwargs = dict(
                model_cls=ExamineResolverContext,
                sub_tasks=[TaskPartition(method_name="fake_subtask", method_kwargs={})],
                processes=workers_count,
                context_kwargs={"mapper": {"local_variable": "is_set"}},
            )

            for subtask_msg in proc_pool.run_subtasks(**subtask_kwargs):
                # there will be only one sub-task return - this isn't tested
                if isinstance(subtask_msg, TaskComplete):
                    # only checking the final/complete message type
                    self.assertEqual(subtask_msg.method_name, "fake_subtask")
                    self.assertEqual(subtask_msg.method_kwargs, {})
                    self.assertFalse(
                        subtask_msg.return_value["build_environment_variable_set"], msg
                    )
                    self.assertTrue(subtask_msg.return_value["local_variable_set"], msg)

    def test_task_message_serialisation(self):
        """
        To and from a string which can be transported across a channel which multiplexes different
        message types.
        """
        sample_message = "Building a dataset has started"
        task_mmessage = TaskLogMessage(msg=sample_message)

        serialised = task_mmessage.to_json()
        self.assertIsInstance(serialised, str)

        message = task_message_factory(serialised)
        self.assertIsInstance(message, TaskLogMessage)
        self.assertEqual(message.msg, sample_message)

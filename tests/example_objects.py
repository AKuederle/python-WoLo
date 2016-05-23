import wolo.log as log
import wolo.task as task
import wolo.parameters as parameters

def test_func(x):
    return "this is a test"

example_log = []
example_log.append(log.TaskLog(index=[0], task_class="0", last_run_success=True))
sublog1 = []
sublog1.append(log.TaskLog(index=[1, "p0", 0], task_class="1_0_0", last_run_success=True))
sublog1.append(log.TaskLog(index=[1, "p0", 1], task_class="1_0_1", last_run_success=True))
sublog2 = []
sublog2.append(log.TaskLog(index=[1, "p1", 0], task_class="1_1_0", last_run_success=False))
sublog2.append(log.TaskLog(index=[1, "p1", 1], task_class="1_1_1", last_run_success=True))
example_log.append([sublog1, sublog2])
example_log.append(log.TaskLog(index=[2], task_class="2", last_run_success=True))

example_flat_view = log.FlatView(example_log)
example_flat_output = {"0": {"index": [0], "task_class": "0", "last_run_success": True, "inputs": {}, "outputs": {}, "info": {}, "last_run": None, "execution_time": None},
                       "1_p0_0": {"index": [1, "p0", 0], "task_class": "1_0_0", "last_run_success": True, "inputs": {}, "outputs": {}, "info": {}, "last_run": None, "execution_time": None},
                       "1_p0_1": {"index": [1, "p0", 1], "task_class": "1_0_1", "last_run_success": True, "inputs": {}, "outputs": {}, "info": {}, "last_run": None, "execution_time": None},
                       "1_p1_0": {"index": [1, "p1", 0], "task_class": "1_1_0", "last_run_success": False, "inputs": {}, "outputs": {}, "info": {}, "last_run": None, "execution_time": None},
                       "1_p1_1": {"index": [1, "p1", 1], "task_class": "1_1_1", "last_run_success": True, "inputs": {}, "outputs": {}, "info": {}, "last_run": None, "execution_time": None},
                       "2": {"index": [2], "task_class": "2", "last_run_success": True, "inputs": {}, "outputs": {}, "info": {}, "last_run": None, "execution_time": None}}


class ExampleTask(task.Task):
    def input(self):
        test_input = parameters.Parameter("test_input", self.args[0])
        return test_input

    def run(self):
        return "test_report"

    def output(self):
        test_output = parameters.Parameter("test_output", self.kwargs["kwarg"])
        return test_output

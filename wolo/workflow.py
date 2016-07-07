from multiprocessing.pool import Pool, ThreadPool
import itertools

from .helper import pretty_print_index, cut_or_pad
from .log import Log, TaskLog

num_of_threads = 4
multicore_switch = False

class Workflow():
    """Provide a Scaffold class to build a workflow.

    To create a Workflow, create a new class with this class as parent.
    Your new class needs the following methods:
        - tasklist: method must return a ordered list of wolo.Task instances.
    Furthermore, it can have the following methods:
        - before : method contains code that need to be run before everything else in the workflow
        - after : method contains code that run after everything else in the workflow

    Notes:
    A workflow can be ran using MyWorkflow().run(). However, you should specify a name for the workflow instance:
    MyWorkflow("MyName").run()
    For each workflow, there will be a logfile created with this name. Therefore, specify a name, if you use a workflow multiple times.
    You can pass Parameter to the workflow instance. They are available as self.args and self.kwargs.

    Example Workflow:

    class MyWorkflow(wolo.Workflow):
        def before(self):
            # Code that needs to run before everything else
            self.myarg = self.arg[0]
            self.myarg2 = self.arg[2]
        def tasktree(self):
            return [MyTask(self.myarg), MyTask2(self.myarg2)]
        def after(self):
            # print some logging information

    """

    def __init__(self, name=None, log_dic=None, *args, **kwargs):
        self._name = type(self).__name__
        if name:
            self._name = "{}_{}".format(self._name, name)
        self.args = args
        self.kwargs = kwargs
        self.log = Log(self._name, log_dic=log_dic)
        self.tasklist = self.tasktree()

    def before(self):
        """Empty method, that can be overwritten by user. Is called on initialization of a workflow."""
        pass

    def after(self):
        """Empty method, that can be overwritten by user. Is called after the workflow ran."""
        pass

    def run(self, return_result=False, _start_level=[]):
        self.before()
        """Run all the tasks returned by the self.tasktree() method."""
        if all(isinstance(step, (list, tuple)) for step in self.tasklist):
            self.tasklist = [self.tasklist]
        success, new_log = _run_tasks(self.tasklist, self.log.log, level=_start_level)
        self.log._set_log(new_log)
        self.afer()
        if return_result is True:
            return success, self.log.log
        else:
            print(success)


def set_Threads(number=4, multicore=False):
    global num_of_threads
    num_of_threads = number
    global multicore_switch
    multicore_switch = multicore


def _run_tasks(task_list, log, level=[]):
    """Run a list of tasks and return the log and success information.

    This is the main function of the module. It is called by the Workflow.run() method. It automatically runs tasks, that can be run in parallel in MultiThreads.
    """
    if not isinstance(log, list):
        log = []
    success = False
    num_of_tasks = len(task_list)
    for i, step, task_log in cut_or_pad(task_list, log, enum=True):
        if num_of_tasks == 1:
            index = level
        else:
            index = level + [i]

        if not task_log:
            log.append(task_log)

        if isinstance(step, Workflow):
            task_success, new_task_log = step.run(return_result=True, _start_level=index)

        elif isinstance(step, (list, tuple)):  # checks if a step is actual a substep (list of steps)
            subtasklist = step
            # checks if current log is a list (as needed for subtasks). if not creates an empty one
            if not isinstance(task_log, list):
                task_log = []

            # checks if all elements in the subtask list are nested --> parallel run, otherwise linear run
            if all(isinstance(step, (list, tuple)) for step in subtasklist):
                sub_index, subtasklist, task_log = zip(*cut_or_pad(subtasklist, task_log, enum=True))
                sub_index = list((index + ["p" + str(i)] for i in sub_index))
                if num_of_threads == 1:
                    list_success, list_log = zip(*itertools.starmap(_run_tasks_wrapper, zip(subtasklist, task_log, sub_index)))
                else:
                    if multicore_switch is True:
                        multi_runner = Pool
                    else:
                        multi_runner = ThreadPool
                    with multi_runner(num_of_threads) as p:
                        list_success, list_log = zip(*p.starmap(_run_tasks_wrapper, zip(subtasklist, task_log, sub_index)))
                new_task_log = list(list_log)
                task_success = all(list_success)

            else:
                task_success, new_task_log = _run_tasks(subtasklist, task_log, level=index)

        else:
            step_class = type(step).__name__
            print(pretty_print_index(index), step_class)
            # checks if current log is really a TaskLog object. if not create an empty one
            if not isinstance(task_log, TaskLog):
                task_log = TaskLog(index=[], task_class=step_class, last_run_success=None)

            new_task_log = step._run(task_log)
            new_task_log.index = index
            task_success = new_task_log.last_run_success

        log[i] = new_task_log
        if task_success is False:
            break

    else:
        success = True
        # This crops the log of tasks, that were removed from the tasktree
        log = log[:i + 1]

    return success, log


def _run_tasks_wrapper(subtasklist, task_log, sub_index):
    '''Needed to make the starmap function work with Multiprocess. Called function as to be importable --> Lambda is not possible.
    Therefore, this wrapper function exists'''
    return _run_tasks(subtasklist, task_log, sub_index)

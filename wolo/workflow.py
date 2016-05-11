from multiprocessing.pool import Pool, ThreadPool
import os
import pickle

from .helper import *


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

    def __init__(self, name=None, *args, **kwargs):
        self._name = type(self).__name__
        if name:
            self._name = "{}_{}".format(self._name, name)
        self.args = args
        self.kwargs = kwargs
        self._logfile = os.path.join(os.getcwd(), ".wolo", ".{}".format(self._name))
        print(self._logfile, ...)
        self._create_logfile()
        self.before()
        self.tasklist = self.tasktree()
        self.log = self._read_log()

    def before(self):
        """Empty method, that can be overwritten by user. is called on initialization of a workflow."""
        pass

    def after(self):
        """Empty method, that can be overwritten by user. is called on initialization of a workflow."""
        pass

    def _create_logfile(self):
        logdir = os.path.dirname(self._logfile)
        os.makedirs(logdir, exist_ok=True)

    def _write_log(self):
        pickle.dump(self.log, open(self._logfile, "wb"))

    def _read_log(self):
        if os.path.isfile(self._logfile):
            return pickle.load(open(self._logfile, "rb"))
        else:
            return None

    def run(self):
        """Run all the tasks returned by the self.tasktree() method."""
        self.log = self._read_log()
        success, self.log = _run_tasks(self.tasklist, self.log)
        self._write_log()

def _run_tasks(task_list, log, level=[]):
    """Run a list of tasks and return the log and success information.

    This is the main function of the module. It is called by the Workflow.run() method. It automatically runs tasks, that can be run in parallel in MultiThreads.
    """
    if not isinstance(log, list):
        log = []
    success = False

    for i, step, task_log in cut_or_pad(task_list, log, enum=True):
        index = level + [i]

        if not task_log:
            log.append(task_log)

        if isinstance(step, (list, tuple)):  # checks if a step is actual a substep (list of steps)
            subtasklist = step
            # checks if current log is a list (as needed for subtasks). if not creates an empty one
            if not isinstance(task_log, list):
                task_log = []

            # checks if all elements in the subtask list are nested --> parallel run, otherwise linear run
            if all(isinstance(step, (list, tuple)) for step in subtasklist):
                sub_index, subtasklist, task_log = zip(*cut_or_pad(subtasklist, task_log, enum=True))
                sub_index = list((index + ["p" + str(i)] for i in sub_index))
                with ThreadPool(4) as p:
                    list_success, list_log = zip(*p.starmap(_run_tasks_wrapper, zip(subtasklist, task_log, sub_index)))
                new_task_log = list(list_log)
                task_success = all(list_success)

            else:
                task_success, new_task_log = _run_tasks(subtasklist, task_log, index)

        else:
            step_class = type(step).__name__
            print(pretty_print_index(index), step_class)
            # checks if current log is really a TaskLog object. if not create an empty one
            if not isinstance(task_log, TaskLog):
                task_log = TaskLog(task_class=step_class, inputs={}, outputs={})

            task_success, new_task_log = step._run(task_log)

        if task_success is False:
            break
        log[i] = new_task_log

    else:
        success = True

    return success, log

def _run_tasks_wrapper(subtasklist, task_log, sub_index):
    '''Needed to make the starmap function work with Multiprocess. Called function as to be importable --> Lambda is not possible.
    Therefore, this wrapper function exists'''
    return _run_tasks(subtasklist, task_log, sub_index)

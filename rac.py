import os
import itertools
from collections import namedtuple
from multiprocessing.pool import Pool, ThreadPool
import pickle
import inspect
import hashlib
import subprocess


class none(): # This exists to allow the use of None as parameter value in inputs
    pass

class Task():
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self._name = Parameter("_name", type(self).__name__)
        self._args = Parameter("_args", self.args)
        self._kwargs = Parameter("_kwargs", self.kwargs)
        self.before()
        self.inputs = self._process(self.input() + [self._name, self._args, self._kwargs])  # passes the class name and arguments as a secret background parameter
        self.outputs = self._process(self.output())

    def before(self):
        pass

    def test(self):
        pass

    def _process(self, para_list):
        name_list = [para.name for para in para_list]
        if not len(set(name_list)) == len(name_list):
            raise Warning("Multiple Parameter have the same name! {}".format(name_list))
        return {para.name: para for para in para_list}

    def _check(self, para_dic, old_values):
        changed = False
        for para in para_dic.values():
            if para.name in old_values:
                old_value = old_values[para.name]
            else:
                old_value = none
            if para._log_value != old_value:
                changed = True
        return changed

    def _rebuild(self, para_dic):
        para_list = para_dic.values()
        for para in para_list:
            para._update()
        return {para.name: para._log_value for para in para_list}

    def _run(self, log):
        """ check dependencies and outputs --> run task --> check success """
        inputs_changed = self._check(self.inputs, log.inputs)
        outputs_changed = self._check(self.outputs, log.outputs)
        print("inputs changed: {}".format(inputs_changed))
        print("outputs changed: {}".format(outputs_changed))
        if inputs_changed is True or outputs_changed is True:
            return self._rerun(log)
        else:
            success = True
            return success, log

    def _rerun(self, log):
        print("rerunning Task...")
        self.report = self.action()
        success = all(self.success())
        if success is True:
            # rebuild log. The log is only updated if the task ran successfully
            log = log._replace(inputs=self._rebuild(self.inputs))
            log = log._replace(outputs=self._rebuild(self.outputs))
        print(success)
        return success, log

class Workflow():
    '''Scarfold class to build a workflow. One Workflow file per name! If no name specified, always the same logfile will be used.
    Parameters will be passed through to use as global, but if name is same, workflowlog will be overwritten!!!'''

    def __init__(self, name=None, *args, **kwargs):
        self._name = type(self).__name__
        if name:
            self._name ="{}_{}".format(self._name, name)
        self.args = args
        self.kwargs = kwargs
        self._logfile = os.path.join(os.getcwd(), ".rac", ".{}".format(self._name))
        self._create_logfile()
        self.before()
        self.tasklist = self.workflow()
        self.log = _read_log()

    def before(self):
        pass

    def after(self):
        pass

    def _create_logfile(self):
        logdir = os.path.dirname(self._logfile)
        os.makedirs(logdir, exist_ok=True)
        open(self._logfile, 'a').close()

    def _write_log(self):
        pickle.dump(self.log, open(self._logfile, "wb"))

    def _read_log(self):
        if os.path.isfile(file):
            return pickle.load(open(self._logfile, "rb"))
        else:
            return None

    def run(self):
        self.log = _read_log()
        success, self.log = _run_tasks(self.tasklist, self.log)
        self._write_log()


class Parameter():
    def __init__(self, name, value, _log_value=None):
        self.name = name
        self.value = value
        if _log_value:
            self._log_value = _log_value
        else:
            self._log_value = value

    def _update(self):
        pass

class File(Parameter):
    def __init__(self, path, name, autocreate=False):
        self.path = path
        self.base = os.path.basename(self.path)
        self.dir = os.path.dirname(self.path)
        self.name = name
        if autocreate is True and not os.path.isfile(self.path):
            self._create()
        self._get_mod_date()
        super().__init__(name=self.name, value=self.path, _log_value=[self.path, self._mod_date])

    def _get_mod_date(self):
        if os.path.isfile(self.path):
            self._mod_date = os.path.getmtime(self.path)
        else:
            self._mod_date = None

    def changed(self):
        old_date = self._mod_date
        self._get_mod_date()
        return not old_date == self._mod_date

    def _create(self):
        os.makedirs(self.dir, exist_ok=True)
        open(self.path, 'a').close()

    def _update(self):
        self._get_mod_date()
        super().__init__(name=self.name, value=self.path, _log_value=[self.path, self._mod_date])

class Source(Parameter):
    def __init__(self, object, name=None):
        self.object = object
        self._get_source()
        if name:
            self.name = name
        else:
            self.name = str(object)
        super().__init__(name=self.name, value=self.source, _log_value=self.hash)

    def _get_source(self):
        self.source = inspect.getsource(self.object)
        self.hash = hashlib.md5(self.source.encode('utf-8')).hexdigest()

    def changed(self):
        old_hash = self._hash
        self._get_source()
        return not old_hash == self._hash

class Self(Source):
    def __init__(self, Self):
        super().__init__(object=Self.__class__, name="Self")


def cmd(*args, **kwargs):
    return subprocess.check_output(*args, **kwargs)


def _run_tasks(task_list, log, level=[]):
    if not isinstance(log, list):
        log = []
    success = False
    for i, step, task_log in _cut_or_pad(task_list, log, enum=True):
        index = level + [i]

        if not task_log: # This might lead to a bug!!!!!!!!!!!!!! OR???????????
            log.append(task_log)

        if isinstance(step, (list, tuple)):  # checks if a step is actual a substep (list of steps)
            subtasklist = step
            # checks if current log is a list (as needed for subtasks). if not creates an empty one
            if not isinstance(task_log, list):
                task_log = []

            # checks if all elements in the subtask list are nested --> parallel run, otherwise linear run
            if all(isinstance(step, (list, tuple)) for step in subtasklist):
                sub_index, subtasklist, task_log = zip(*_cut_or_pad(subtasklist, task_log, enum=True))
                sub_index = list((index + ["p" + str(i)] for i in sub_index)) # some cleanup needed
                with ThreadPool(4) as p:
                    list_success, list_log = zip(*p.starmap(_run_tasks_wrapper, zip(subtasklist, task_log, sub_index)))
                new_task_log = list(list_log)
                task_success = all(list_success)

            else:
                task_success, new_task_log = _run_tasks(subtasklist, task_log, index)


        else:
            step_class = type(step).__name__
            print(_pretty_print_index(index), step_class)
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


def _pretty_print_index(index):
    return "".join(["[{}]".format(i) for i in index])


def _cut_or_pad(master, slave, enum=False):
    for i in range(len(master)):
        try:
            slave_val = slave[i]
        except:
            slave_val = None
        if enum is True:
            yield i, master[i], slave_val
        else:
            yield master[i], slave_val


def _run_tasks_wrapper(subtasklist, task_log, sub_index):
    '''Needed to make the starmap function work with Multiprocess. Called function as to be importable --> Lambda is not possible.
    Therefore, this wrapper function exists'''
    return _run_tasks(subtasklist, task_log, sub_index)


TaskLog = namedtuple("TaskLog", ["task_class", "inputs", "outputs"])

import os
import itertools
from collections import namedtuple
from multiprocessing.pool import ThreadPool
import pickle
import inspect
import hashlib
import subprocess


class none(): # This exists to allow the use of None as paramter value in inputs
    pass

class Task():
    def __init__(self, *args, **kwargs):
        self.__args__ = args
        self.__kwargs__ = kwargs
        self.before()
        self.inputs = self.input()
        self.outputs = self.output()

    def before(self):
        pass

    def test(self):
        pass

    def _check(self, para_list, old_values):
        changed = False
        for para in para_list:
            if para.name in old_values:
                old_value = old_values[para.name]
            else:
                old_value = none
            if para._log_value != old_value:
                changed = True
        return changed

    def _rebuild(self, para_list):
        for para in para_list:
            para._update()
        return {para.name: para._log_value for para in para_list}

    def _run(self, log):
        """ check dependecies and outputs --> run task --> check success """
        inputs_changed = self._check(self.inputs, log.inputs)
        outputs_changed = self._check(self.outputs, log.outputs)
        print(self.__args__[0])
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
            # rebuild log. The log is only updated if the task ran succesfully
            log = log._replace(inputs=self._rebuild(self.inputs))
            log = log._replace(outputs=self._rebuild(self.outputs))
        return success, log


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
    def __init__(self, path, name=None, autocreate=False):
        self.path = path
        self.base = os.path.basename(self.path)
        self.dir = os.path.dirname(self.path)
        if name:
            self.name = name
        else:
            self.name = path
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



def _write_log(log):
    pickle.dump(log, open(os.path.join(os.getcwd(), ".rac"), "wb"))

def _read_log():
    file = os.path.join(os.getcwd(), ".rac")
    if os.path.isfile(file):
        return pickle.load(open(os.path.join(os.getcwd(), ".rac"), "rb"))
    else:
        return None

def run(workflow):
    steps = workflow()
    log = _read_log()
    success, new_log = _run_tasks(steps, log, "main")
    print(success)
    _write_log(new_log)


def _run_tasks(task_list, log, name=""):
    if not log:
        log = []
    success = [True]  # needed so that first task runs
    step_log_list = itertools.zip_longest(task_list, log)
    for i, (step, task_log) in enumerate(step_log_list):
        if success[-1] is not True:
            break
        if isinstance(step, (list, tuple)):
            subtasklist = step
            if not task_log:
                task_log = []
                log.append(task_log)
            if all(isinstance(step, (list, tuple)) for step in subtasklist):
                print("Entering parallel sublist at {}".format(i))
                with ThreadPool(6) as p:
                    list_success, list_log = zip(*p.starmap(lambda x, y: _run_tasks(x, y), itertools.zip_longest(subtasklist, task_log)))
                    log[i] = list(list_log)
                    success.append(all(list_success))
            else:
                list_success, list_log = _run_tasks(step, task_log, "sublist {}".format(i))
                log[i] = list_log
                success.append(list_success)
                pass
        else:
            step_class = type(step).__name__
            print("running {} at position {} in {}".format(step_class, i, name))
            if not task_log:
                task_log = TaskLog(task_class=step_class, inputs={}, outputs={})
                log.append(task_log)
                task_success, new_task_log = step._run(task_log)
            elif not hasattr(task_log, "task_class") or not task_log.task_class == step_class:
                print("New Task {} at {}.\nForce rerun...".format(step_class, i))
                task_log = TaskLog(task_class=step_class, inputs={}, outputs={})
                task_success, new_task_log = step._rerun(task_log)
            else:
                task_success, new_task_log = step._run(task_log)

            log[i] = new_task_log
            print("success: {}".format(task_success))
            success.append(task_success)
    return all(success), log


TaskLog = namedtuple("TaskLog", ["task_class", "inputs", "outputs"])

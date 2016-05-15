from pathlib import Path
import pickle

from .helper import pretty_print_index


class TaskLog():
    def __init__(self, index, task_class, inputs={}, outputs={}, last_run_success=None):
        self.index = index
        self.task_class = task_class
        self.inputs = inputs
        self.ouputs = outputs
        self.last_run_success = last_run_success

    def __getitem__(self, selection):
        return {key: self.__dict__[key] for key in selection}

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    def __repr__(self):
        values = ", ".join(["{} = {}".format(key, value) for key, value in dict(self).items()])
        return "TaskLog({})".format(values)

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)

    def _asdict(self):
        i = pretty_print_index(self.index, style="underscore")
        value = dict(self)
        return {i: value}


class Log():
    """Wolo will create all the logs is a subfolder of the current working dir called .wolo.
    Be aware, that you have to call the workflow file from the same working directory every time
    """
    def __init__(self, name):
        self._log_dic = Path.cwd() / ".wolo"
        self._log_path = self._log_dic / ".{}".format(name)
        self._log = None

    @property
    def log(self):
        if not self._log:
            self._log = self._load()
        return self._log

    @log.setter
    def log(self, new_log):
        self._log = new_log
        self._write()

    def view(self):
        """Generate a view opject from the current log, which can be manipulated and analysed."""
        return View(self.log)

    def _load(self):
        if self._log_path.is_file():
            return pickle.load(self._log_path.open("rb"))
        else:
            return []

    def _write(self):
        self._log_dic.mkdir(parents=True, exist_ok=True)
        pickle.dump(self._log, self._log_path.open("wb"))


class View():
    def __init__(self, log):
        self.log = log
        self._flattened = None

    def as_dict(self):
        return dict(self.log)

    @property
    def flat(self):
        if not self._flattened:
            self._flattened = FlatView(self.log)
        return self._flattened

    def simple_tree(self, formatter=lambda x: x.task_class):
        return list(_recursive_iterate_log(self.log, formatter))


class FlatView():
    def __init__(self, log):
        self.log = _flatten_log(log)

    def __repr__(self):
        return self.log

    def __iter__(self):
        for element in self.log:
            yield element


def _flatten_log(L):
    """Flattens a nested log"""
    for i in L:
        if isinstance(i, TaskLog):
            yield i._asdict()
        else:
            yield from _flatten_log(i)


def _recursive_iterate_log(L, func):
    for i in L:
        if isinstance(i, TaskLog):
            yield func(i)
        else:
            yield list(_recursive_iterate_log(i, func))

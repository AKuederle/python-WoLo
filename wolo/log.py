from pathlib import Path
import pickle


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

    def _load(self):
        if self._log_path.is_file():
            return pickle.load(self._log_path.open("rb"))
        else:
            return None

    def _write(self):
        self._log_dic.mkdir(parents=True, exist_ok=True)
        pickle.dump(self._log, self._log_path.open("wb"))


class View():
    def __init__(self, log):
        self.log = log

    def as_dict(self):
        return dict(self.log)

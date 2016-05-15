from pathlib import Path
import pickle


class Log():
    """Wolo will create all the logs is a subfolder of the current working dir called .wolo.
    Be aware, that zou have to call the workflow file from the same working directory everytime
    """
    def __init__(self, name):
        self._log_dic = Path.cwd() / ".wolo"
        if not self._log_dic.is_dir():
            self._log_dic.mkdir(parents=True)
        self._log_path = self._log_dic / ".{}".format(name)
        self.log = self._load()

    def update(self):
        self.log = self._load()
        return self

    def _load(self):
        if self._log_path.is_file():
            return pickle.load(self._log_path.open("rb"))
        else:
            return None

    def _write(self):
        pickle.dump(self.log, self._log_path.open("wb"))


class View():
    def __init__(self, log):
        self.log = log

    def as_dict(self):
        return dict(self.log)

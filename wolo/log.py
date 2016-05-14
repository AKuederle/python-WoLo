from pathlib import Path
import pickle


class Log():
    def __init__(self, path):
        self._log_path = Path(path)
        self.log = self._load()

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

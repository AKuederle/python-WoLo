import os
import inspect
import hashlib

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
    def __init__(self, name, path, autocreate=False):
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

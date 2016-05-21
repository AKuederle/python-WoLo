from pathlib import Path
import inspect
import hashlib


class Parameter():
    """General Parameter class to register a Value as input or output parameter.
    name: The name the parameter is referenced with
    value: The value of the parameter. It will be saved and can be retrieved with myparameter.value
    _log_value: The value which is used to check if a Parameter changed. By default it is the value.

    Notes: You can easily extend the Parameter class by manipulating the _log_value. See the Files class for example.
    """
    def __init__(self, name, value, _log_value=None):
        self.name = name
        self.value = value
        if _log_value:
            self._log_value = _log_value
        else:
            self._log_value = value

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and self._log_value == other._log_value)

    def _update(self):
        """The update function is important for all Parameters, that can change during a run (like a outputfile).
        It is called after the run, but before the Parameter values are saved, so that always the newest values are saved.
        """
        pass


class File(Parameter):
    """Special Parameter Class for files. It uses the timestamp to check for updates.
    name: name for the file/parameter
    path: path to the file
    autocreate: if True it is checked, if the file exists. If not, it will be created. Interesting for output files.

    Notes: The .changed() Method can be used to check if a the timestamp of a file is changed. This can be interesting in the success method.
    """
    def __init__(self, name, path, autocreate=False):
        self.path = Path(path)
        self.parent = self.path.parent
        self.name = name
        if autocreate is True and not self.path.is_file():
            self._create()
        self._mod_date = self._get_mod_date()
        super().__init__(name=self.name, value=str(self.path), _log_value=[str(self.path), self._mod_date])

    def _get_mod_date(self):
        if self.path.is_file():
            mod_date = self.path.stat().st_mtime
        else:
            mod_date = None
        return mod_date

    def changed(self):
        """Check if the timestamp is updated in between runs"""
        return not self._mod_date == self._get_mod_date()

    def _create(self):
        self.parent.mkdir(parents=True, exist_ok=True)
        self.path.open('a').close()

    def _update(self):
        self._mod_date = self._get_mod_date()
        super().__init__(name=self.name, value=self.path, _log_value=[str(self.path), self._mod_date])


class Source(Parameter):
    """Special Parameter Class for sourcecode. It uses a hashvalue of the source to check if it changed.
    object: Python object you want to have the source from. inspect.getsource() is used for that.
    name: Name of the parameter. If none it is the string repr of the object.

    Notes: The .changed() Method can be used to check if a the source is changed compared to the last run.
    """
    def __init__(self, object, name=None):
        self.object = object
        self._hash = self._get_source()
        if name:
            self.name = name
        else:
            self.name = str(object)
        super().__init__(name=self.name, value=None, _log_value=self._hash)

    def _get_source(self):
        source = inspect.getsource(self.object)
        return hashlib.md5(source.encode('utf-8')).hexdigest()

    def changed(self):
        """Check if the source is updated in between runs"""
        return not self._hash == self._get_source()


class Self(Source):
    """Special Parameter Class for the Sourcecode of the step definition. Can also be used to get the source of a class based on an instance.
    Self: class instance (can be self, when used inside an Class definition)
    name: name of the Parameter. Default is "Self"

    Note: Usage as input parameter:
    Self = wolo.Self(self)
    """
    def __init__(self, Self, name="Self"):
        super().__init__(object=Self.__class__, name=name)

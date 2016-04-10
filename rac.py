import os

class Task():
    def __init__(self, *args, **kwargs):
        self.__args__ = args
        self.__kwargs__ = kwargs

    def _inputs_change(self):
        inputs = self.input()
        changed = False
        for dependecy in inputs:
            old_value = self._lookup(dependecy.name)
            if dependecy.changed(old_value) == True:
                changed = True
        return changed

    def _lookup(self, name):
        return 1394446668.0

    def _do(self, old_dependecies):
        """ check dependecies and outputs --> run task --> check sucess """

        


class Parameter():
    def __init__(self, name, value, _log_value=None):
        self.name = name
        self.value = value
        if _log_value:
            self._log_value = _log_value
        else:
            self._log_value = value

    def changed(self, old_value):
        return not self._log_value == old_value

        

class File(Parameter):
    def __init__(self, filename, name=None):
        self.path = filename
        if name:
            name = name
        else:
            name = filename
        self._mod_date = os.path.getmtime(self.path)
        super().__init__(name=name, value=self.path, _log_value=self._mod_date)


import subprocess

from .parameters import Parameter



class Task():
    """Provide a scaffold class for a Task.

    To create a task, create a new class with this class as parent.
    Your new class needs the following methods:
        - input : method must return a list of wolo-parameters. Reformatted inputs are stored in self.inputs
        - output : method must return a list of wolo-parameters. Reformatted outputs are stored in self.outputs
        - action : method that contains the action that the task should perform. The optional return parameter is stored in self.report
        - success : method must return True or a list which evaluate to True for the Task to be considered successful
    Furthermore, it can have the following methods:
        - before : method contains code that need to be run before everything else in the tasks
        - after : method contains code that run after everything else in the Task

    Further notes:
        All arguments passed to a custom Task are stored in self.args and self.kwargs

    Example Task class:
    import wolo
    class MyTask(wolo.Task):
        def before(self):
            myfile = self.args[0]

        def input(self):
            file = wolo.File("myfile", "filepath")  # wolo.file returns an object with the file path as file.path and its mod date as __str()__
            Self = wolo.Self(self) # gets the sourcecode of the Task itself as input
            return [Self, file]

        def action(self):
            # do awesome stuff
            return "Everything is great" # this will be stored in self.report

        def output(self):
            return [wolo.File("outfile", "fielpath")]

        def success(self):
            outputs_changed = self.outputs["outfile"].changed() # checks if output file changed
            return [outputs_changed]

        def after(self):
            print(self.report) # print the action results
    """

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
        """Empty method, that can be overwritten by user. is called on initialization of a task."""
        pass

    def after(self):
        """Empty method, that can be overwritten by user. is called on initialization of a task."""
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
                if para._log_value != old_value:
                    changed = True
            else:
                changed = True
        return changed

    def _rebuild(self, para_dic):
        para_list = para_dic.values()
        for para in para_list:
            para._update()
        return {para.name: para._log_value for para in para_list}

    def _run(self, log):
        """Check dependencies and outputs --> run task --> check success."""
        inputs_changed = self._check(self.inputs, log.inputs)
        outputs_changed = self._check(self.outputs, log.outputs)
        print("inputs changed: {}".format(inputs_changed))
        print("outputs changed: {}".format(outputs_changed))
        if inputs_changed is True or outputs_changed is True or log.last_run_success is False:
            log = self._rerun(log)
        return log

    def _rerun(self, log):
        print("rerunning Task...")
        self.report = self.action()
        success = all(self.success())
        if success is True:
            # rebuild log. The log is only updated if the task ran successfully
            log = log._replace(inputs=self._rebuild(self.inputs))
            log = log._replace(outputs=self._rebuild(self.outputs))
            log = log._replace(last_run_success=True)
        else:
            log = log._replace(last_run_success=False)
        print(success)
        return log




def cmd(*args, **kwargs):  # need to figure out where to put this
    return subprocess.check_output(*args, **kwargs)

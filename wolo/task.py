import subprocess
import timeit
import time

from .helper import TaskProperty, convert_return


class Task():
    """Provide a scaffold class for a Task.

    To create a task, create a new class with this class as parent.
    Your new class needs the following methods:
        - input : method must return a list of wolo-parameters. Reformatted inputs are stored in self.inputs
        - output : method must return a list of wolo-parameters. Reformatted outputs are stored in self.outputs
        - action : method that contains the action that the task should perform. The optional return parameter is stored in self.report
        - success : method must return True or a list which evaluate to True for the Task to be considered successful
    Furthermore, it can have the following methods:
        - before : method contains code that need to be run before everything else in the tasks.
        - after : method contains code that run after everything else in the Task. Can return WoLo Parameters, which will be stored in the log file

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
            report = wolo.Parameter("report", self.report)
            return report  # This will be stored in self.info and stored in the Log file
    """

    def __init__(self, *args, **kwargs):
        self.args = convert_return(args)
        self.kwargs = kwargs
        self.before()
        self.inputs = self._process(convert_return(self.input()))
        self.outputs = self._process(convert_return(self.output()))

    def before(self):
        """Empty method, that can be overwritten by user. Is called on initialization of a task."""
        pass

    def after(self):
        """Empty method, that can be overwritten by user. Is called after the action method of the task."""
        pass

    def _process(self, para_list):
        name_list = [para.name for para in para_list]
        if not len(set(name_list)) == len(name_list):
            raise Warning("Multiple Parameter have the same name! {}".format(name_list))
        return TaskProperty({para.name: para for para in para_list})

    def _check(self, para_dic, old_values):
        changed = False
        for para in para_dic:
            if para.name in old_values:
                if para._log_value != old_values[para.name]:
                    changed = True
            else:
                changed = True
        return changed

    def _rebuild(self, para_dic):
        for para in para_dic:
            para._update()
        return {para.name: para._log_value for para in para_dic}

    def _run(self, log):
        """Check dependencies and outputs --> run task --> check success."""
        inputs_changed = self._check(self.inputs, log.inputs)
        outputs_changed = self._check(self.outputs, log.outputs)
        print("inputs changed: {}".format(inputs_changed))
        print("outputs changed: {}".format(outputs_changed))
        if inputs_changed is True or outputs_changed is True or log.last_run_success is not True:
            log = self._rerun(log)
        return log

    def _rerun(self, log):
        print("rerunning Task...")
        start_time = timeit.default_timer()
        self.report = self.action()
        log.execution_time = timeit.default_timer() - start_time
        log.last_run = time.ctime(int(time.time()))
        success = all(convert_return(self.success()))
        after = self.after()
        if after:
            log.info = self._rebuild(self._process(convert_return(after)))
        if success is True:
            # rebuild log. The log is only updated if the task ran successfully
            log.inputs = self._rebuild(self.inputs)
            log.outputs = self._rebuild(self.outputs)
            log.last_run_success = True
        else:
            log.last_run_success = False
        print(success)
        return log


def cmd(*args, **kwargs):  # need to figure out where to put this
    return subprocess.check_output(*args, **kwargs)

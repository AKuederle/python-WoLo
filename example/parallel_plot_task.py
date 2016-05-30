"""This is a short example of how to use WoLo to control your plotting and to
get a report over all the plotted figures.

This example should demonstrate an easy way to with a lot of different plots,
that have to be plotted saved and potentially regenerated because of changes
in the plotting code or the data.

My personal workflow for doing that without WoLo was to create a file with a lot
of functions, each representing a specific plot I wanted to generate for e.g. a
final report. Having each plot as an function already allows to manually recreate
an figure if you made changes. However, having 50+ plots, you will loose track
pretty quickly. The only way to see which plots you have, how they are named and
when they were generated is by looking at the generated files itself. A issue I
had with this workflow, was that for reports I always needed the figures in a
defined size, which I would change, if I changed my minded about the layout of
the report. The only way to figure out, which size my figure currently was, was
by guessing based on the report.

WoLo eases this process, by automatically rerunning the plotting task, which
have changed inputs, save the metadata and the plotting parameters you passed to
the functions. At the end it creates a pandas DataFrame as report showing you
all the important information about your figures. This report can be
regenerated, without running the the Workflow itself or could be saved to file
as an easy log of all the figures you have available.

Thinking in terms of a report this example could be appended, by automatically
triggering the generation of e.g. a Latex report, if one of the plots has been
regenerate. Using the generated log and some Latex tricks you could even
generate warnings, if some pictures are not used in your texfile.

This only shows a part of a full workflow. However, as WoLo allows to use an
existing workflow as subworkflow of an bigger woirkflow, it would be really easy
to integrate it in a workflow covering the full data pipeline.
"""

###############################################################################
# These lines are not needed if you run the script with WoLo installed on your system
import os
import sys
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath('..'))
###############################################################################

import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

import numpy as np


import wolo

# These are some example plotting functions. In a real live scenario, these would
# go into a sperate file and would be imported into your Wolo file.

def plot_line(slope, outputfile_name):
    plt.close('all')
    x = np.linspace(0, 10)
    plt.plot(x, slope*x)
    plt.savefig(outputfile_name)

def plot_parabola(vertex, outputfile_name):
    plt.close('all')
    x = np.linspace(0, 10)
    plt.plot(x, x**2 + vertex)
    plt.savefig(outputfile_name)


class Plot(wolo.Task):
    """This is the Main Task of our Workflow. We will pass the imported plot
    function itself as an argument together with some plotting parameters and
    the output filename (The actual path is specified in the Task). All these
    objects will be treeted as inputs/outputs by wolo and tracked inbetween
    runs.

    The task is consideresd successful, if the timestamp of the output file was
    changed during the execution of the action method.
    """
    def input(self):
        plot_function = wolo.Source("function", self.args[0])
        Self = wolo.Self(self)
        plot_parameter = wolo.Parameter("parameter", self.args[1])
        return plot_function, Self, plot_parameter

    def action(self):
        """Calls the actual plotting function using the passed inputs"""
        self.inputs.function.value(self.inputs.parameter.value, self.outputs.outfile.value)

    def output(self):
        output_file = wolo.File("outfile", './results/{}.png'.format(self.args[2]), autocreate=True)
        return output_file

    def success(self):
        """Checks if the timestamp of the output file changed."""
        return self.outputs.outfile.changed()


wolo.set_Threads(number=4, multicore=True) # Set the multicore properties
class PlotFlow(wolo.Workflow):
    """This is the actual Workflow we want to execute. It calls the Task we
    generated multiple times with different parameters By nesting each of the
    Tasks one level, we ensure, that the tasks will eb executed in parallel by
    WoLo.
    """
    def tasktree(self):
        tree = []
        tree.append([Plot(plot_line, 1, 'line_1')])
        tree.append([Plot(plot_line, 5, 'line_5')])
        tree.append([Plot(plot_parabola, 3, 'parabola_3')])
        tree.append([Plot(plot_parabola, 10, 'parabola_10')])
        return tree

if __name__ == "__main__":
    workflow = PlotFlow()
    workflow.run()
    log = workflow.log.flat.cols(["task_class", "execution_time", "last_run"])
    log = log.col_from_prop("outputs", "outfile")
    log = log.col_from_prop("inputs", "function")
    log = log.col_from_prop("inputs", "parameter")
    log = log.to_pandas()
    print(log)

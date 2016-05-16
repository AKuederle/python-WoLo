=======
WoLo
=======

WoLo is a lightweight Python tool to create and **Lo**\ g **Wo**\ rkflows for dynamic scientific data analysis. It has a very small footprint and is entirely written and configurable using plane Python. This makes is easy to use, but doesn't sacrifice flexibility and extensibility.
With WoLo you can keep the overview over your data analysis while making it easily shareable and understandable.

It has (will have) all the features you expect from a workflow manager:

- Specify tasks with inputs and outputs
- Specify workflows with parallel execution of not depended tasks
- Only rerun steps, which have changed inputs and outputs
- Get all information you need about the state of your data analysis \*

But wait, there is more:

- monitor a wide range of inputs and outputs:

    + Files
    + Input Parameters
    + Source Code of custom Python modules and functions
    + Source Code of the Step-configuration itself

- Easily extend and adapt the functionality with a few lines of Python-code
- Keep track and safe custom logs alongside with your workflow \*
- Run meta-analysis on your workflow using the full power of Python \*

\* not fully implemented yet

from collections import namedtuple

def pretty_print_index(index):
    '''Turn the index into a formated string which is shown a output'''
    return "".join(["[{}]".format(i) for i in index])


def cut_or_pad(master, slave, enum=False):
    '''Return a ziped opjects with from two different length lists.

    The slave list is either cut or padded with "None" to fit the length of master. If enum is set to True,
    the index is also returned as part of the iterator.'''
    for i in range(len(master)):
        try:
            slave_val = slave[i]
        except:
            slave_val = None
        if enum is True:
            yield i, master[i], slave_val
        else:
            yield master[i], slave_val

TaskLog = namedtuple("TaskLog", ["task_class", "inputs", "outputs", "last_run_success"])

class none(): # This exists to allow the use of None as parameter value in inputs
    pass

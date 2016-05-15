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


def flatten_log(L):
    """Flattens a nested log"""
    for i in L:
        if isinstance(i, TaskLog):
            yield i
        else:
            yield from flatten_log(i)


TaskLog = namedtuple("TaskLog", ["index", "task_class", "inputs", "outputs", "last_run_success"])
TaskLog.__new__.__defaults__ = ((), None, {}, {}, False)

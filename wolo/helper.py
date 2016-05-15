

def pretty_print_index(index, style="brackets"):
    '''Turn the index into a formated string which is shown a output.
    Two stzles are available:
    - brackets: [1][1][1]
    - underscore: 1_1_1
    '''
    if style == "brackets":
        return "".join(["[{}]".format(i) for i in index])
    elif style == "underscore":
        return "_".join([str(i) for i in index])


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

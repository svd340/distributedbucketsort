import inspect
import time

CALLS = "CALLS"
TOTALEXEC = "TOTALEXECUTIONTIME"
EXECTIMES = "EXECTIMES"

# the datastructure to store all execution times
timethisdata = {}


def timethis(f):
    # This function times and counts all execution calls for the decorated function.
    name = f.__qualname__

    timethisdata[name] = {CALLS: 0, TOTALEXEC: 0, EXECTIMES: []}

    def wrapper(*args, **kwargs):
        t = time.time()
        timethisdata[name][CALLS] += 1
        return_data = f(*args, **kwargs)
        exectime = time.time() - t

        timethisdata[name][EXECTIMES].append(exectime)
        timethisdata[name][TOTALEXEC] += exectime
        return return_data

    return wrapper


def print_timethis(name='NODE'):
    # this function prints all data from the execution stack
    for key, value in timethisdata.items():
        print("[EXECUTION DATA {:>5}:{:>22}]totalexectime:{:.6f}\tcalls:{}".format(name, key, value[TOTALEXEC],
                                                                                   value[CALLS]))

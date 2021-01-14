import time


def time_it(function):
    def wrapper():
        start_time = time.time()
        output = function()
        execution_time = time.time() - start_time
        print(f'Execution time: {execution_time:.5f} s.')
        return output

    return wrapper

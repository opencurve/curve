import time
import sys
import math
from multiprocessing import Pool
import multiprocessing


time_of_run = 0.1
percent_cpu = sys.argv[1] # Should ideally replace this with a smaller number e.g. 20
cpu_time_utilisation = float(percent_cpu)/100
on_time = time_of_run * cpu_time_utilisation
off_time = time_of_run * (1-cpu_time_utilisation)
core = multiprocessing.cpu_count()



def single_core_stress(i):
    while True:
        start_time = time.clock()
        while time.clock() - start_time < on_time:
            math.factorial(100) #Do any computation here
        time.sleep(off_time)


def multiprocess_concurrent_method():
    p = Pool(core)
    p.map(single_core_stress, range(1,core+1))

if __name__ == '__main__':
    multiprocess_concurrent_method()
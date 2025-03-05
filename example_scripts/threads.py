# creates 100 numbers to get through with 5 threads.
# each thread will announce its presence, wait a second, do an action, wait another second, then announce its exit

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

# do threading
def worker_func(thread_num):
    print(f"thread #{thread_num} starting\n")
    sleep(1)
    # print(f"thread #{thread_num} working\n")
    sleep(1)
    # print(f"thread #{thread_num} exiting\n")



# define number of threads
completed = 0
num_threads = 100
threadpool_size = 5
threads_to_make = []
for i in range(num_threads):
    threads_to_make.append(i)


#make threadpool and execute command
with ThreadPoolExecutor(threadpool_size) as executor:
    futures = [executor.submit(worker_func, thread_num) for thread_num in threads_to_make]

    for _ in as_completed(futures):
        # size = executor._work_queue.qsize() + 10
        # print(f"around {size} threads remaining\n") # to see thread progress in terminal
        completed+=1
        print(f"{completed} threads completed, {num_threads-completed} threads remaining\n")

print("all threads complete")
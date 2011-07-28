from multiprocessing import Process, Queue
from multiprocessing import Value
import multiprocessing
import os
import time, random


def info(title):
    print title
    print 'module name:', __name__
    print 'parent process:', os.getppid()
    print 'process id:', os.getpid()

def f(name, queue):
  #info(name)
  time.sleep((random.random() * 10) % 10)
  print 'hello ' + name + ', joint: ' + str(joint_var.value) + ', elapsed: ' + str(time.time()-start_time)
  joint_var.value += 1


if __name__ == '__main__':
  start_time = time.time()
  queue = Queue()
  joint_var = Value('i',0)

  for i in range(10):
    queue.put(Process(target=f, args=(str(i),queue)).start())

  ##queue.close()
  #queue.join_thread()
  #print "Total Elapsed: " + str(time.time() - start_time)
  print str(multiprocessing.cpu_count() *2)

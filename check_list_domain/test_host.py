import requests
import queue
import threading

w = queue.Queue()
File = 'hosts.txt'

with open(File, 'rt') as infile:
    for element in infile:
        w.put(element)

progress = 'Queue: {:<' + str(len(str(w.qsize()))) + '}'


class domainTest(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = False

    def run(self):
        while not w.empty():
            host = w.get()
            r = requests.get(str('https://' + host).strip())
            print(r.status_code, host)


threads = list()
for i in range(0, 10):
    t = domainTest()
    t.start()
threads.append(t)
for t in threads:
    t.join()

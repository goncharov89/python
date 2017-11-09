import requests
import json
import queue
import threading
import csv
import os

csvFiles = list()
for file in os.listdir():
    if file.endswith(".csv"):
        csvFiles.append(file)

w = queue.Queue()
true_list = ('Доступен', 'В корзине')
url = ""
token = ""
header = {'Content-Type': 'application/json', 'token': token}
count = 500

for csvFile in csvFiles:

    with open(csvFile, 'rt') as infile:
                infile_csv = csv.DictReader(infile, delimiter=';')
                for in_row in infile_csv:
                    element = list()
                    print(in_row)
                    element.append(in_row['Номер'])
                    if in_row['Статус'] not in true_list:
                        element.append(False)
                    else:
                        element.append(True)
                    w.put(element)

progress = 'Queue: {:<' + str(len(str(w.qsize()))) + '}'


class domainThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = False

    def run(self):
        while not w.empty():
            body_json = []
            for i in range(0, min(500, w.qsize())):
                unit = w.get()
                body_json.append({
                    'msisdn': unit[0].strip(),
                    'availability': unit[1],
                    'expirationDate': None})
            r = requests.put(
                url,
                data=json.dumps(body_json),
                headers=header)
            print(progress.format(w.qsize()), end='\r', flush=True)
            if r.status_code != 200:
                exit(r.status_code)


threads = list()
for i in range(0, 5):
    t = domainThread()
    t.start()
threads.append(t)
for t in threads:
    t.join()

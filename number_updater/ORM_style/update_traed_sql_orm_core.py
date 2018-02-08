import requests
import json
import queue
import threading
from sqlalchemy import *
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from urllib.parse import quote


w = queue.Queue()
true_list_int = (1, 2)
url = ""
token = ""
header = {'Content-Type': 'application/json', 'token': token}
count = 500
branch_region = (95,)
server = ''
database = ''
username = ''
password = ''
driver = '{ODBC Driver 13 for SQL Server}'

params = quote('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
mssql = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
metadata = MetaData(mssql)

Numbers = Table('Number', metadata,
                Column('MSISDN', BIGINT, primary_key=True),
                Column('CategoryID', INTEGER),
                Column('TypeID', INTEGER),
                Column('BranchID', INTEGER),
                Column('NumberStatusID', INTEGER),
                Column('CreatedDate', DATETIME),
                Column('ModifyStatusDate', DATETIME),
                Column('ID', UNIQUEIDENTIFIER),
                Column('ReservedUntil', DATETIME),
                Column('ReserveToken', UNIQUEIDENTIFIER)
                )

metadata.create_all(mssql)
conn = mssql.connect()

s = select([Numbers.c.MSISDN, Numbers.c.NumberStatusID]).where(Numbers.c.BranchID.in_(branch_region))
res = conn.execute(s)

for row in res:
    element = list()
    element.append(str(row.MSISDN))
    if row.NumberStatusID not in true_list_int:
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
            for i in range(0, min(count, w.qsize())):
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


print(progress.format(w.qsize()), end='\r', flush=True)
threads = list()
for i in range(0, 10):
    t = domainThread()
    t.start()
threads.append(t)
for t in threads:
    t.join()

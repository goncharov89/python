
from sqlalchemy import *
from urllib.parse import quote
import queue
import threading

w = queue.Queue()

# MSSQL connection param
branch_region = (95, )
server = ''
database = ''
username = ''
password = ''
driver = '{ODBC Driver 13 for SQL Server}'
count = 10000
params = quote('DRIVER=' + driver + ';PORT=1433;SERVER=' + server +
               ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
mssql = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
mssql.echo = False
# MYSQL connection param
mysql = create_engine('mysql+pymysql://py:py@10.78.251.183/py', pool_size=20, max_overflow=30, echo=False)

metadata = MetaData(mssql)
Numbers = Table('Number', metadata,
                Column('MSISDN', BIGINT, primary_key=True),
                Column('CategoryID', INTEGER),
                Column('TypeID', INTEGER),
                Column('BranchID', INTEGER),
                Column('NumberStatusID', INTEGER),
                Column('CreatedDate', DATETIME),
                Column('ModifyStatusDate', DATETIME),
                Column('ID', String),
                Column('ReservedUntil', DATETIME),
                Column('ReserveToken', String)
                )

metadata.create_all(mssql)

metadata_mysql = MetaData(mysql)
Numbers_mysql = Table('Number_mysql', metadata_mysql,
                      Column('MSISDN', BIGINT, primary_key=True),
                      Column('CategoryID', INTEGER),
                      Column('TypeID', INTEGER),
                      Column('BranchID', INTEGER),
                      Column('NumberStatusID', INTEGER),
                      Column('CreatedDate', DATETIME),
                      Column('ModifyStatusDate', DATETIME),
                      Column('ID', String(100)),
                      Column('ReservedUntil', DATETIME),
                      Column('ReserveToken', String(100)),
                      mysql_charset='utf8'
                      )

metadata_mysql.create_all(mysql)
Numbers_mysql.create(mysql, checkfirst=True)

conn = mssql.connect()

s = select([Numbers]).where(
    Numbers.c.BranchID.in_(branch_region))
res = conn.execute(s)
i = 0
for row in res:
    cortege = list()
    cortege.append(row.MSISDN)
    cortege.append(row.CategoryID)
    cortege.append(row.TypeID)
    cortege.append(row.BranchID)
    cortege.append(row.NumberStatusID)
    cortege.append(row.CreatedDate)
    cortege.append(row.ModifyStatusDate)
    cortege.append(row.ID)
    cortege.append(row.ReservedUntil)
    cortege.append(row.ReserveToken)
    w.put(cortege)

progress = 'Queue: {:<' + str(len(str(w.qsize()))) + '}'


class CopyTableThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = False

    def run(self):
        i = 0
        conn_mysql = mysql.connect()
        trans = conn_mysql.begin()
        while not w.empty():
            row = w.get()
            ins = Numbers_mysql.insert().values(MSISDN=row[0],
                                                CategoryID=row[1],
                                                TypeID=row[2],
                                                BranchID=row[3],
                                                NumberStatusID=row[4],
                                                CreatedDate=row[5],
                                                ModifyStatusDate=row[6],
                                                ID=row[7],
                                                ReservedUntil=row[8],
                                                ReserveToken=row[9])
            conn_mysql.execute(ins)
            i = i + 1
            if i >= count:
                trans.commit()
                i = 0
                print(progress.format(w.qsize()), end='\r', flush=True)
                conn_mysql.close
                conn_mysql = mysql.connect()
                trans = conn_mysql.begin()
        trans.commit()
        conn_mysql.close


print(progress.format(w.qsize()), end='\r', flush=True)

threads = list()
for i in range(0, 10):
    t = CopyTableThread()
    t.start()
threads.append(t)
for t in threads:
    t.join()


from sqlalchemy import *

from urllib.parse import quote

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
mssql.echo = True

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
metadata.create_all(mssql, checkfirst=True)



mysql = create_engine('mysql+pymysql://py:py@10.78.251.183/py', echo=True)
metadata_mysql = MetaData(mysql)
Numbers_mysql = Table('Number_mysql', metadata_mysql,
                      Column('MSISDN', Integer, primary_key=True),
                      Column('CategoryID', Integer),
                      Column('TypeID', Integer),
                      Column('BranchID', Integer),
                      Column('NumberStatusID', Integer),
                      Column('CreatedDate', DateTime),
                      Column('ModifyStatusDate', DateTime),
                      Column('ID', String(100)),
                      Column('ReservedUntil', DateTime),
                      Column('ReserveToken', String(100)),
                      mysql_charset='utf8'
                      )

metadata_mysql.create_all(mysql)
exit(0)


Numbers_mysql.create(mysql, checkfirst=True)

conn = mssql.connect()

s = select([Numbers]).where(
    Numbers.c.BranchID.in_(branch_region))
res = conn.execute(s)
i = 0
conn_mysql = mysql.connect()
trans = conn_mysql.begin()
for row in res:
    #trans = conn_mysql.begin()
    ins = Numbers_mysql.insert().values(MSISDN=row.MSISDN,
                                        CategoryID=row.CategoryID,
                                        TypeID=row.TypeID,
                                        BranchID=row.BranchID,
                                        NumberStatusID=row.NumberStatusID,
                                        CreatedDate=row.CreatedDate,
                                        ModifyStatusDate=row.ModifyStatusDate,
                                        ID=row.ID,
                                        ReservedUntil=row.ReservedUntil,
                                        ReserveToken=row.ReserveToken)
    conn_mysql.execute(ins)
    i = i + 1
    if i >= count:
        trans.commit()
        trans = conn_mysql.begin()
        i = 0
conn_mysql.close()
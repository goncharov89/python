#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import requests
import configparser
import os
from requests.auth import HTTPBasicAuth
import cx_Oracle
import socket

socket.setdefaulttimeout(0.1)

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(
    os.path.realpath(__file__)), "main.conf"))

URL = config['URLs']['URL']
URL_ETALON = config['URLs']['URL_ETALON']

user = config['AUTH']['DYN_ADMIN_LOGIN']
password = config['AUTH']['DYN_ADMIN_PASSWORD']


DB_SCHEMA = config['PUB_DB']['db_schema']
DB_USER = config['PUB_DB']['db_user']
DB_PASS = config['PUB_DB']['db_pass']
DB_HOST = config['PUB_DB']['db_host']
DB_PORT = config['PUB_DB']['db_port']
DB_SERVICE_NAME = config['PUB_DB']['db_service_name']
CONNECT_STRING = '%s/%s@%s:%s/%s' % (DB_USER,
                                     DB_PASS, DB_HOST, DB_PORT, DB_SERVICE_NAME)

sql_list_ID = list()
sql_conn = cx_Oracle.Connection(CONNECT_STRING)
sql_conn.current_schema = DB_SCHEMA

sql_cursor = sql_conn.cursor()
sql_cursor.execute(
    """
        SELECT * FROM PUB.site_configuration WHERE is_head = 1 AND version_deleted != 1 
    """)
for row in sql_cursor.fetchall():
    sql_list_ID.append(row[8])

res_sql = set(sql_list_ID)

r = requests.get(URL, proxies={
                 "https": config['PROXIES']['HTTP']}, auth=HTTPBasicAuth(user, password))
res = r.text

pattern = r"<li>prod[0-9]*\:\n(.+)"

all_li = re.findall(pattern, res)

all_site_id = list()
for line in all_li:
    result = re.sub(r'\[', '', re.sub(r'],.*', '',
                                      re.sub(r'.*siteIds=', '', line)))
    if result.find(',') != -1:
        result_split = result.split(',')
        for line_split in result_split:
            all_site_id.append(line_split)
    else:
        all_site_id.append(result)

res = set(all_site_id)

finish_set = res & res_sql

# print(finish_set)

DB_SCHEMA = config['STAGE_DB']['db_schema']
DB_USER = config['STAGE_DB']['db_user']
DB_PASS = config['STAGE_DB']['db_pass']
DB_HOST = config['STAGE_DB']['db_host']
DB_PORT = config['STAGE_DB']['db_port']
DB_SERVICE_NAME = config['STAGE_DB']['db_service_name']
CONNECT_STRING = '%s/%s@%s:%s/%s' % (DB_USER,
                                     DB_PASS, DB_HOST, DB_PORT, DB_SERVICE_NAME)

sql_conn = cx_Oracle.Connection(CONNECT_STRING)
sql_conn.current_schema = DB_SCHEMA

sql_cursor = sql_conn.cursor()

sql = 'SELECT t2.SLUG FROM t2_region t1 INNER JOIN T2_AREA t2 ON t1.AREA_ID = t2.AREA_ID WHERE t1.site_id IN (%s)' % ','.join(
    "'" + str(x) + "'" for x in list(finish_set))

slug_list = list()
sql_cursor.execute(sql)
for row in sql_cursor.fetchall():
    slug_list.append(row[0])

r = requests.get(URL_ETALON, proxies={
                 "https": config['PROXIES']['HTTP']}, auth=HTTPBasicAuth(user, password))
etalon = r.text

pattern = r'<span style=\'white-space:pre\'>\{(.+)\}</span>'

etalon_1 = re.findall(pattern, etalon)
list_of_etalon = str(etalon_1[0]).split(', ')

mapping = list()

for i in range(0, len(list_of_etalon)):
    elements = str(list_of_etalon[i]).split('=')
    if slug_list.count(elements[1] + 'shop') > 0:
        mapping.append(elements[0] + '=' + elements[1] + 'shop')
        mapping.append(list_of_etalon[i])
    else:
        if slug_list.count(elements[1]) > 0:
            mapping.append(list_of_etalon[i])

super_string = ''.join(str(x) + ', ' for x in mapping)
print(super_string.strip())

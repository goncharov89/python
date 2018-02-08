#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import requests
import json
from requests_ntlm import HttpNtlmAuth
import xml.etree.ElementTree as ET
from socket import socket
from OpenSSL import SSL
from datetime import datetime
from ConfigParser import ConfigParser
import os
import urllib3
import threading
import Queue


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# переносим все переменные из конфига
Config = ConfigParser()
Config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "cert.conf"))
url_auth = Config.get("URL", 'url_auth')
url = Config.get("URL", 'url')
login = Config.get("AUTH", 'login')
password = Config.get("AUTH", 'pass')
wQueue = Queue.Queue()
message = list()


def send_slack(msg):
    if msg != '':
        response = requests.post(
                                 Config.get("URL", 'webhook_url'),
                                 proxies={"https": "http://t2ru-ds-proxy-01:80"},
                                 data=json.dumps({"text": msg}),
                                 headers={'Content-Type': 'application/json'}
                                )
        if response.status_code != 200:
            raise ValueError('Request to slack returned an error %s, the response is:\n%s' % (
                response.status_code, response.text))


class domainThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = False
    def run(self):
        global message
        while not wQueue.empty():
            domain = wQueue.get()
            ssl_connection = SSL.Connection(SSL.Context(SSL.SSLv23_METHOD), socket())
            time = Config.get("BASIC", 'timeout')
            ssl_connection.settimeout(int(time))
            try:
                ssl_connection.connect((domain, 443))
            except:
                continue
            ssl_connection.settimeout(None)
            ssl_connection.set_connect_state()
            ssl_connection.set_tlsext_host_name(domain.encode('UTF-8'))
            try:
                ssl_connection.do_handshake()
            except:
                continue
            ssl_connection.close()
            cert = ssl_connection.get_peer_certificate()
            valid_date = datetime.strptime(
                cert.get_notAfter()[:8].decode("utf-8"), "%Y%m%d")
            date_delta = int((valid_date - datetime.now()).days)
            # Формируем сообщение
            if (date_delta < int(Config.get("BASIC", "days")) and
                date_delta > int(Config.get("BASIC", "remind_depth"))):
                message.append(domain + ' SSL expire at ' + \
                    str(valid_date.date()) + ' in ' + str((valid_date -
                    datetime.now()).days) + ' days')


# Пытаемся авторизоваться и получить токен доступа дял SharePoint
r = requests.post(url_auth, auth=HttpNtlmAuth(
    login, password), verify=False)
if r.status_code != 200:
    send_slack("Не удалось авторизоваться")
    exit(1)

tree = ET.fromstring(r.text)
string = tree[1].text
pos = string.find(',')
token = string[0:pos]
headers = {'user-X-RequestDigest': token}
# Теперь имея токен можем получить xml страинцу с weblandscape, затем сосздаем список доменов
res = requests.get(url, auth=HttpNtlmAuth(
    login, password), verify=False, headers=headers)
if res.status_code != 200:
    send_slack("Не удалось получить список weblandscape")
    exit(1)

tr = ET.fromstring(res.text)
res = []
for child in tr:
    if child.tag == '{http://www.w3.org/2005/Atom}entry':
        host = child[6][0][0].text
        host = host.replace('http://', '').replace('https://', '').replace('www.', '').replace(
            '*', 'msk').replace('.ru/', '.ru').replace('http:/', '').replace('https:/', '')
        if not host.endswith('.corp.tele2.ru'):
            wQueue.put(host)

threads = list()
for i in range(0, min((int(Config.get('BASIC', 'threads'))), wQueue.qsize())):
    t = domainThread()
    t.start()
    threads.append(t)
for t in threads:
    t.join()

# print('\n'.join(message))
send_slack('\n'.join(message))

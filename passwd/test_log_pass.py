import csv
import os
import requests
from requests.auth import HTTPBasicAuth

# const
url_auth = 'https://'

csvFiles = list()
for file in os.listdir():
    if file.endswith(".csv"):
        csvFiles.append(file)

print(csvFiles)

for csvFile in csvFiles:

    with open(csvFile, 'rt', newline="") as infile:
        infile_csv = csv.DictReader(infile, delimiter=';')
        for in_row in infile_csv:
            r = requests.get(url_auth, auth=HTTPBasicAuth(
                str(in_row['login']).strip(), str(in_row['pass']).strip()))
            print(in_row['login'], in_row['pass'], r.status_code)

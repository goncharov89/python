#!/usr/bin/env python

from subprocess import run

run('htpasswd -b /tmp/.htpasswd 000 000').std
a = run('passgen -n 1 -l 6 --upper').stdout
print(a)
auth = list()
f = open('log_pass.txt', 'w')
with open('logins.txt', 'rt') as infile:
    for in_row in infile:
        password = run('passgen -n 1 -l 6 --upper').stdout
        f.write(in_row.strip() + ' ' + password)
        #run ('htpasswd -b /tmp/.htpasswd ' + in_row.strip() + ' ' + password)
f.close()

with open('log_pass.txt', 'rt') as filename:
    for row in filename:
        run('htpasswd -b /tmp/.htpasswd ' + row.strip())

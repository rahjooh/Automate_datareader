#!/usr/bin/env python

from ftplib import FTP
from time import sleep
import os
import re

try:
    ftp = FTP('10.100.136.53')
    ftp.login('ftpac','123*qaz')
    ftp.encoding = 'utf-8'

    ftp.cwd('/.')  # change to root directory for downloading
    f = '/94-95/FTP Contents/Bank-Iran/Isc2Bank/tat941101/ACTINFO_ACT2_1101.zip'
    file_name = f.replace('/', '_')  # use path as filename prefix, with underscores
    ftp.retrbinary('RETR ' + f, open('act.zip', 'wb').write)
except:
    print('oh dear.')
    ftp.quit()

ftp.quit()
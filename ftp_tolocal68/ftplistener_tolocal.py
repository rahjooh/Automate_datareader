from subprocess import Popen,PIPE,STDOUT
import multiprocessing
import random
from time import sleep
import datetime
from StringIO import StringIO
import sys
import os
import os.path
import ConfigParser
import sqlite3
from ftplib import FTP
import paramiko
import socket

os_type = None
def ScpToRemote(config, fullPath, file_name):

    # Generate new filename for sending to SSH
    tmp = fullPath.split("/")
    tmp = tmp[len(tmp) - 2]
    y = '13' + tmp[3:5]
    password = tmp.replace("tat", "13")
    if "isc" in fullPath.lower():
        password = "isc" + password

    return

# FTPIsFile for checking file or directory
def FTPIsFile(filename):
    current = ftp.pwd()
    try:
        ftp.cwd(filename)
    except:
        ftp.cwd(current)
        return True
    ftp.cwd(current)
    return False

# FTPGetDirectory for getting file list from directory
def FTPGetDirectory(ln):
    global my_dirs
    global my_files
    global files_size
    global folders_ts
    global files_ts
    global os_type

    #strdate = ln[0:8]
    #tmp = strdate.split('-')
    #y = tmp[2]
    #m = tmp[0]
    #d = tmp[1]
    #strdate = '20' + y + m + d

    #strtime = ln[10:19]
    #tmp = strtime.split(':')
    #h = tmp[0]
    #m = tmp[1][0:2]
    #strtime = h + m
    is_file = True
    #ts = strdate + strtime
    if os_type == "unix":
        if ln.startswith("drw"):
            is_file = False
        cols = ln.split()
        i = -1
        cnt = 0
        for s in cols:
            i = i + 1
            if(s.isdigit() == True):
                cnt = cnt + 1
                if cnt == 3:
                    break
        start = i + 2
        last = len(cols) - 1
        last = len(cols)
        if(start == last):
            objname = cols[start]
        else:
            objname = cols[start:last]  # file or directory name
            objname = ' '.join(objname)
        fullPath = os.path.join(curdir, objname)
        if (is_file != True):
            DirectoryContainWith = config.get('Main', 'ftpDirectoryContainWith')
            if DirectoryContainWith in objname:
                my_dirs.append(objname)
        else:
            fileFormat = config.get('Main', 'ftpFileFormat')
            fileFormat = set(fileFormat.split(","))
            ftpFilenameStartWith = config.get('Main', 'ftpFilenameStartWith')
            ftpFilenameStartWith = set(ftpFilenameStartWith.split(","))
            if objname.lower().endswith(tuple(fileFormat)):
                if objname.startswith(tuple(ftpFilenameStartWith)):
                    fullPath = os.path.join(curdir, objname)
                    my_files.append(fullPath)  # full path
                    #size = cols[i]
                    #files_size[fullPath] = size
                    #files_ts[fullPath] = ts
        return
    dir_str = '       <DIR>          '
    if ln.find(dir_str) != -1:
        start = ln.find(dir_str) + len(dir_str)
        objname = ln[start:]
        DirectoryContainWith = config.get('Main', 'ftpDirectoryContainWith')
        if DirectoryContainWith in objname:
            my_dirs.append(objname)
    else:
        cols = ln.split()
        i = -1
        for s in cols:
            i = i + 1
            if(s.isdigit() == True):
                break

        start = i + 1
        last = len(cols) - 1
        last = len(cols)
        if(start == last):
            objname = cols[start]
        else:
            objname = cols[start:last]  # file or directory name
            objname = ' '.join(objname)

            fileFormat = config.get('Main', 'ftpFileFormat')
            fileFormat = set(fileFormat.split(","))
            ftpFilenameStartWith = config.get('Main', 'ftpFilenameStartWith')
            ftpFilenameStartWith = set(ftpFilenameStartWith.split(","))
            if objname.lower().endswith(tuple(fileFormat)):
                if objname.startswith(tuple(ftpFilenameStartWith)):
                    fullPath = os.path.join(curdir, objname)
                    my_files.append(fullPath)  # full path
                    size = cols[i]
                    files_size[fullPath] = size
                    ts = "test"
                    files_ts[fullPath] = ts

# FTPCheckingDirectory for get directory list
def FTPCheckingDirectory(adir):

    global my_dirs
    global my_files
    global curdir

    my_dirs = []
    gotdirs = []

    curdir = ftp.pwd()

    print("going to change to directory " + adir + " from " + curdir)
    try:
        ftp.cwd(adir)
    except:
        print("can not going to change to directory " + adir + " from " + curdir)
        return

    curdir = ftp.pwd()
    print("now in directory: " + curdir)
    ftp.retrlines('LIST', FTPGetDirectory)
    gotdirs = my_dirs
    # print("Total files found so far: " + str(len(my_files)) + ".")
    # sleep(1)
    for subdir in gotdirs:
        my_dirs = []
        FTPCheckingDirectory(subdir)  # recurse
    ftp.cwd('..')  # back up a directory when done her


if __name__ == '__main__':

    # Global variable
    files_size = {}
    folders_ts = {}
    files_ts = {}
    my_dirs = []
    my_files = []
    curdir = ''

    rootPath = os.path.realpath(__file__)
    temp = rootPath.split('/')
    rootPath = rootPath.replace(temp[-1],"")

    if os.path.isfile('ftp_tolocal.config'):
        config = ConfigParser.ConfigParser()
        config.readfp(open(r'ftp_tolocal.config'))
    else:
        sys.exit('config file not found.')

    #  Config
    host = config.get('Main', 'ftphost')
    username = config.get('Main', 'ftpusername')
    password = config.get('Main', 'ftppassword')
    root = config.get('Main', 'ftproot')
    startPath = config.get('Main', 'ftpstartPath')
    startPath = startPath.split(',')

    jobs = []
    ##################3
    try:
        ftp = FTP(host)
        ftp.encoding = 'utf-8'
        conn = ftp.login(username, password)
        rand_name = "rgg"
        ftp.mkd(rand_name)
    except Exception as e:
        if str(e) == "550 Can't create directory. Permission denied":
            os_type = "unix"
        else:
            os_type = "windows"
    print("Your FTP Server is " + os_type)
    # Connect FTP Server and retrive files
    try:
        ftp = FTP(host)
        ftp.encoding = 'utf-8'
        conn = ftp.login(username, password)

        if isinstance(startPath, (list,)):
            for item in startPath:
                if item != '':
                    print(item)
                    FTPCheckingDirectory(item)
        else:
            FTPCheckingDirectory(startPath)
        # change to root directory for downloading
        ftp.cwd(root)

        add = my_files
        count = 0
        # looping for download from ftp connection to local
        for f in add:
            while len(jobs) > 2:
                jobs = [job for job in jobs if job.is_alive()]
                print(str(len(jobs)) + ' jobs, wait for free job queue')
                sleep(2)

            if FTPIsFile(f) == True :
                fullPath = f
                DirectoryContainWith = config.get('Main', 'ftpDirectoryContainWith')
                if DirectoryContainWith not in fullPath:
                    continue
                print('getting ' + fullPath)

                try:
                    # calculate file size
                    size = ftp.size(fullPath)
                    file_name = f.split('/')
		    #new_name = file_name[-2]
		    #tmp = new_name.split("t")
		    #ans = tmp[2]
                    file_name = file_name[-1]
                    # retrive file from ftp connection to local
                    LocalFile = open(rootPath + "data/" + file_name,"wb")
                    ftp.retrbinary('RETR ' + f ,LocalFile.write)
		    #old_file = os.path.join("/home/big/ftp_tolocal/ftproot_tolocal/",file_name)
		    #tmp = file_name.split(".")
		    #new_file = os.path.join("/home/big/ftp_tolocal/ftproot_tolocal/",tmp[0] + "_" + ans + ".zip")
		    #print(new_file)
		    #os.rename(old_file, new_file)
                    LocalFile.close()

                    """
                    r = StringIO()
                    ftp.retrbinary('RETR ' + f , r.write)
                    """

                    count = count + 1
                    sleep(1)

                except:
                    e = sys.exc_info()[0]
                    if str(e) == "<class 'ftplib.error_perm'>":
                        print(f + " is uploading ... \n")
                        continue
        ftp.quit()
    except Exception as e:
        print(e)
        if '530 User cannot log in.' in e:
            print('FTP username or password wrong.')
        ftp.quit()
        sys.exit()

    # Iterate through the list of jobs and remove one that are finished, checking every second.
    while len(jobs) > 0:
        jobs = [job for job in jobs if job.is_alive()]
        sleep(1)

    print('*** All jobs finished ***')

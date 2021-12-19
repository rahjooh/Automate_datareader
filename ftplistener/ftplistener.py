# based on example code from https://pymotw.com/2/multiprocessing/basics.html
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
import uuid
import re
import jdatetime
#from lib.SSHConnection import SSHConnection
class SSHConnection(object):

    def __init__(self, host, username, password, port=22):
        """Initialize and setup connection"""
        self.sftp = None
        self.sftp_open = False

        # open SSH Transport stream
        self.transport = paramiko.Transport((host, port))
        self.transport.connect(username=username, password=password)

    def _openSFTPConnection(self):
        """
        Opens an SFTP connection if not already open
        """
        if not self.sftp_open:
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            self.sftp_open = True

    def get(self, remote_path, local_path=None):
        """
        Copies a file from the remote host to the local host.
        """
        self._openSFTPConnection()
        self.sftp.get(remote_path, local_path)

    def put(self, local_path, remote_path=None):
        """
        Copies a file from the local host to the remote host
        """
        self._openSFTPConnection()
        self.sftp.put(local_path, remote_path)

    def close(self):
        """
        Close SFTP connection and ssh connection
        """
        if self.sftp_open:
            self.sftp.close()
            self.sftp_open = False
        self.transport.close()
def getRandstr():
    return str(uuid.uuid4())

def generateName(path,randstr):
    filename = path.split('/')
    filename = filename[-1]
    return rootPath + 'ftproot' + '/' + randstr + '_' + filename

def generatePassword(FtpPath):

    filename = FtpPath.split('/')
    filename = filename[-1]

    # get month and day from path
    md = re.search('tat(.*)/', FtpPath)
    if md:
        md = md.group(1)
        y = str(jdatetime.date.today().strftime("%Y"))[0:2]
        dt = y + md
        password = ''
        if "isc" in FtpPath.lower():
            password = "isc" + dt
        return password
    else:
        return False

def generateDate(FtpPath):

    filename = FtpPath.split('/')
    filename = filename[-1]

    # get month and day from path
    md = re.search('tat(.*)/', FtpPath)
    md = md.group(1)
    y = str(jdatetime.date.today().strftime("%Y"))[0:2]
    dt = y + md
    return dt

def generatesshFileName(localPath,dt):

    filename = localPath.split('/')
    filename = filename[-1]
    return dt + '_' + filename
    #.replace(filename[len(filename) - 8:],'.zip')

def sendMessage(id, text):

    server_address = ('10.100.136.60', 5556)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address)

    except socket.error, e:
        if 'Connection refused' in e:
            print('Connection refused')
            return 'Connection refused'
    try:
        message = 'add' + ' ' + id + ' ' + text
        sock.sendall(message)
        try:
            response = sock.recv(4096)
        except:
            print('Oh noes! %s' % sys.exc_info()[0])
            return False
    finally:
        sock.close()
    return True

def connectToSocketServer(servicehost, serviceport):

    server_address = (servicehost, int(serviceport))
    try:
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Connect the socket to the port where the server is listening
        sock.connect(server_address)
        return sock
    except socket.error, e:
        if 'Connection refused' in e:
            return False

def ScpToRemote(config, FtpPath, localPath, randstr):

    servicehost = config.get('service', 'host')
    serviceport = config.get('service', 'port')

    try_ = 1
    sock = connectToSocketServer(servicehost, serviceport)
    while (sock ==  False and try_ < 3):
        sendMessage(randstr, 'Connection refused')
        try_ = try_ + 1
        sendMessage(randstr, 'connecting to service ... #' + str(try_))
        sock = connectToSocketServer(servicehost, serviceport)
        sleep(1)

    if(sock ==  False):
        os.remove(localPath)
        sendMessage(randstr, 'remove from local...')
        return 'Connection refused'

    sendMessage(randstr, ' Scp ' + localPath + ' to service client.')

    # SSH config
    host = config.get('service', 'host')
    username = config.get('service', 'username')
    password = config.get('service', 'password')
    sshpath = config.get('service', 'path')
    if not sshpath.endswith('/'):
        sshpath = sshpath + '/'

    # Make SSH Connection
    ssh = SSHConnection(host, username, password)

    # Generate new filename for sending to SSH
    if localPath.endswith('.zip'):
        password = generatePassword(FtpPath)
        if password == False:
            print('no detect password')
            return

        dt = generateDate(FtpPath)
        sshFileName = generatesshFileName(localPath, dt)

        # SCP file to remote
        ssh.put(localPath, sshpath + sshFileName)

        os.remove(localPath)
        sendMessage(randstr, "remove from local...")
        response = ''
        try:
            # Send data
            message = 'unzip' + ' ' + sshFileName + ' ' + password + ' ' + randstr
            sock.sendall(message)
            # Look for the response
            try:
                response = sock.recv(4096)
            except:
                sendMessage(randstr, 'Oh noes! %s' % sys.exc_info()[0])
                return
        finally:
            sendMessage(randstr, 'closing socket')
            sock.close()

        if 'Done!' in response:
            try:
                con = sqlite3.connect(rootPath + 'ftp.db', timeout=10)
                cur = con.cursor()
                cur.execute("INSERT INTO tbl_dir VALUES ('%s')" % FtpPath)
                con.commit()
            except sqlite3.Error as e:
                sendMessage(randstr, "Database error: %s" % e)
            except Exception as e:
                sendMessage(randstr, "Exception in _query: %s" % e)
            finally:
                sendMessage(randstr, 'write to db and tik')
                if con:
                    con.close()

        sendMessage(randstr, 'Finish Scp file to remote.')

    return

# FTPIsFile for waiting
def waitToTomorrow():
    tomorrow = datetime.datetime.replace(datetime.datetime.now() + datetime.timedelta(days=1),
                         hour=0, minute=0, second=0)
    delta = tomorrow - datetime.datetime.now()
    sleep(delta.seconds)

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

    strdate = ln[0:8]
    tmp = strdate.split('-')
    y = tmp[2]
    m = tmp[0]
    d = tmp[1]
    strdate = '20' + y + m + d

    strtime = ln[10:19]
    tmp = strtime.split(':')
    h = tmp[0]
    m = tmp[1][0:2]
    strtime = h + m

    ts = strdate + strtime
    #print(ts)
    dir_str = '       <DIR>          '
    if ln.find(dir_str) != -1:
        start = ln.find(dir_str) + len(dir_str)
        objname = ln[start:]
        DirectoryContainWith = config.get('ftp', 'filePath')
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

            fileFormat = config.get('ftp', 'fileType')
            fileFormat = set(fileFormat.split(","))

            ftpFilenameStartWith = config.get('ftp', 'fileName')
            ftpFilenameStartWith = set(ftpFilenameStartWith.split(","))
            if objname.lower().endswith(tuple(fileFormat)):
                if objname.startswith(tuple(ftpFilenameStartWith)):
                    fullPath = os.path.join(curdir, objname)
                    my_files.append(fullPath)  # full path
                    size = cols[i]
                    files_size[fullPath] = size
                    files_ts[fullPath] = ts

# FTPCheckingDirectory for get directory list
def FTPCheckingDirectory(adir):

    global my_dirs
    global my_files
    global curdir

    my_dirs = []
    gotdirs = []

    curdir = ftp.pwd()

    #print("going to change to directory " + adir + " from " + curdir)
    try:
        ftp.cwd(adir)
    except:
        print("can not going to change to directory " + adir + " from " + curdir)
        return

    curdir = ftp.pwd()
    #print("now in directory: " + curdir)
    ftp.retrlines('LIST', FTPGetDirectory)
    gotdirs = my_dirs
    # print("Total files found so far: " + str(len(my_files)) + ".")
    # sleep(1)
    for subdir in gotdirs:
        my_dirs = []
        FTPCheckingDirectory(subdir)  # recurse
    ftp.cwd('..')  # back up a directory when done her

def bytes_2_human_readable(number_of_bytes):
    if number_of_bytes < 0:
        raise ValueError("!!! number_of_bytes can't be smaller than 0 !!!")

    step_to_greater_unit = 1024.

    number_of_bytes = float(number_of_bytes)
    unit = 'bytes'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'KB'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'MB'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'GB'

    if (number_of_bytes / step_to_greater_unit) >= 1:
        number_of_bytes /= step_to_greater_unit
        unit = 'TB'

    precision = 1
    number_of_bytes = round(number_of_bytes, precision)

    return str(number_of_bytes) + ' ' + unit

if __name__ == '__main__':


    rootPath = os.path.realpath(__file__)
    temp = rootPath.split('/')
    rootPath = rootPath.replace(temp[-1],"")

    if len(sys.argv) > 1:
        randcod = sys.argv[1]
        file = open('/home/big/ftplistener/' + str(randcod) +'.txt', "w")
        file.write("randcode of ftplistener is : " + randcod)
        file.close()

    # Global variable

    files_size = {}
    folders_ts = {}
    files_ts = {}
    my_dirs = []
    my_files = []
    curdir = ''

    if os.path.isfile(rootPath + '/config/ftp.config'):
        config = ConfigParser.ConfigParser()
        config.readfp(open(r'' + rootPath + '/config/ftp.config'))
    else:
        sys.exit('config file not found.')

    if not os.path.exists(rootPath + 'ftproot'):
        os.makedirs(rootPath + 'ftproot')

    #  Config
    host = config.get('ftp', 'host')
    username = config.get('ftp', 'username')
    password = config.get('ftp', 'password')
    root = config.get('ftp', 'root')
    startPath = config.get('ftp', 'startPath')
    startPath = startPath.split(',')

    #   Connect to DB
    conn = sqlite3.connect(rootPath + 'ftp.db', timeout=10)
    c = conn.cursor()
    c.execute('''CREATE TABLE if not exists tbl_dir 
                     (path text)''')

    jobs = []

    # Connect FTP Server and retrive files
    try:
        ftp = FTP(host)
        ftp.encoding = 'utf-8'
        conn = ftp.login(username, password)

        if isinstance(startPath, (list,)):
            for item in startPath:
                if item != '':
                    FTPCheckingDirectory(item)
        else:
            FTPCheckingDirectory(startPath)
        # change to root directory for downloading
        ftp.cwd(root)

        # get history

        sql = "SELECT path FROM tbl_dir WHERE path in (%s)" % ','.join(map("'{0}'".format, my_files))
        c.execute(sql)
        all_rows = c.fetchall()

        dbls = []
        for row in all_rows:
            dbls.append(str(row[0]))

        add = []
        rem = []

        if len(dbls) == 0:
            add = my_files
        else:
            for item in my_files:
                if item not in dbls:
                    add.append(item)
                else:
                    rem.append(item)

        count = 0
        # looping for download from ftp connection to local
        add.sort()
        for FtpPath in add:
            while len(jobs) > 2:
                jobs = [job for job in jobs if job.is_alive()]
                print(str(len(jobs)) + ' jobs, wait for free job queue')
                sleep(2)

            print('getting ' + FtpPath)
            if FTPIsFile(FtpPath) == True :
                DirectoryContainWith = config.get('ftp', 'filePath')
                if DirectoryContainWith not in FtpPath:
                    continue
                try:
                    # calculate file size
                    size = ftp.size(FtpPath)

                    file_name = FtpPath.split('/')
                    file_name = file_name[-1]

                    randstr = getRandstr()
                    localPath = generateName(FtpPath,randstr)

                    sendMessage(randstr, 'RETR FtpPath to local ...')
                    sendMessage(randstr, "file size : " + bytes_2_human_readable(size))
                    # retrive file from ftp connection to local
                    LocalFile = open(localPath, "wb")
                    ftp.retrbinary('RETR ' + FtpPath ,LocalFile.write)
                    LocalFile.close()

                    sendMessage(randstr, 'Finish RETR FtpPath to local ...')

                    """
                    r = StringIO()
                    ftp.retrbinary('RETR ' + f , r.write)
                    """

                    multiProcess = True
                    if multiProcess == True:
                        p = multiprocessing.Process(target=ScpToRemote, args=(config, FtpPath, localPath, randstr))
                        jobs.append(p)
                        p.start()
                    else:
                        ScpToRemote(config, FtpPath, localPath, randstr)

                    count = count + 1
                    sleep(1)

                except:
                    e = sys.exc_info()[0]
                    if str(e) == "<class 'ftplib.error_perm'>":
                        print(FtpPath + " is uploading ... \n")
                        continue
        ftp.quit()
    except Exception, e:
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

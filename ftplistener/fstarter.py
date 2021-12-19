import multiprocessing
from time import sleep
import time
import sys
import os
import os.path
import paramiko
import json
import logging
import socket
import datetime
import subprocess
import signal
# ================================================================
# class MySSH
# ================================================================
class MySSH:

    '''
    Create an SSH connection to a server and execute commands.
    Here is a typical usage:

        ssh = MySSH()
        ssh.connect('host', 'user', 'password', port=22)
        if ssh.connected() is False:
            sys.exit('Connection failed')

        # Run a command that does not require input.
        status, output = ssh.run('uname -a')
        print 'status = %d' % (status)
        print 'output (%d):' % (len(output))
        print '%s' % (output)

        # Run a command that does requires input.
        status, output = ssh.run('sudo uname -a', 'sudo-password')
        print 'status = %d' % (status)
        print 'output (%d):' % (len(output))
        print '%s' % (output)
    '''

    def __init__(self, compress=True, verbose=False):
        '''
        Setup the initial verbosity level and the logger.

        @param compress  Enable/disable compression.
        @param verbose   Enable/disable verbose messages.
        '''
        self.ssh = None
        self.transport = None
        self.compress = compress
        self.bufsize = 65536

        # Setup the logger
        self.logger = logging.getLogger('MySSH')
        self.set_verbosity(verbose)

        fmt = '%(asctime)s MySSH:%(funcName)s:%(lineno)d %(message)s'
        format = logging.Formatter(fmt)
        handler = logging.StreamHandler()
        handler.setFormatter(format)
        self.logger.addHandler(handler)
        self.info = self.logger.info

    def __del__(self):
        if self.transport is not None:
            self.transport.close()
            self.transport = None

    def connect(self, appid, hostname, username, password, port=22):
        '''
        Connect to the host.

        @param hostname  The hostname.
        @param username  The username.
        @param password  The password.
        @param port      The port (default=22).

        @returns True if the connection succeeded or false otherwise.
        '''
        self.info('connecting %s@%s:%d' % (username, hostname, port))
        self.hostname = hostname
        self.username = username
        self.port = port
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(hostname=hostname,port=port,username=username,password=password)
            self.transport = self.ssh.get_transport()
            self.transport.use_compression(self.compress)
            self.info('succeeded: %s@%s:%d' % (username,hostname,port))
            #print('succeeded: %s@%s:%d' % (username,hostname,port))
        except socket.error as e:
            self.transport = None
            self.info('failed: %s@%s:%d: %s' % (username, hostname, port, str(e)))
            #print('failed: %s@%s:%d: %s' % (username, hostname, port, str(e)))
            fail.append(appid)
        except paramiko.BadAuthenticationType as e:
            self.transport = None
            self.info('failed: %s@%s:%d: %s' % (username, hostname, port, str(e)))
            #print('failed: %s@%s:%d: %s' % (username, hostname, port, str(e)))
            fail.append(appid)
        return self.transport is not None

    def run(self, appid, cmd, input_data=None, timeout=10):
        '''
        Run a command with optional input data.

        Here is an example that shows how to run commands with no input:

            ssh = MySSH()
            ssh.connect('host', 'user', 'password')
            status, output = ssh.run('uname -a')
            status, output = ssh.run('uptime')

        Here is an example that shows how to run commands that require input:

            ssh = MySSH()
            ssh.connect('host', 'user', 'password')
            status, output = ssh.run('sudo uname -a', '<sudo-password>')

        @param cmd         The command to run.
        @param input_data  The input data (default is None).
        @param timeout     The timeout in seconds (default is 10 seconds).
        @returns The status and the output (stdout and stderr combined).
        '''
        self.info('running command: (%d) %s' % (timeout, cmd))
        if self.transport is None:
            self.info('no connection to %s@%s:%s' % (str(self.username), str(self.hostname), str(self.port)))
            print('no connection to %s@%s:%s' % (str(self.username), str(self.hostname), str(self.port)))
            fail.append(appid)
            return -1, 'ERROR: connection not established\n'

        # Fix the input data.
        input_data = self._run_fix_input_data(input_data)

        # Initialize the session.
        self.info('initializing the session')
        session = self.transport.open_session()
        session.set_combine_stderr(True)
        session.get_pty()
        session.exec_command(cmd)
        output = self._run_poll(appid, session, timeout, input_data)
        status = session.recv_exit_status()
        self.info('output size %d' % (len(output)))
        self.info('status %d' % (status))
        return status, output
    def connected(self):
        return self.transport is not None
    def set_verbosity(self, verbose):
        if verbose > 0:
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.ERROR)
    def _run_fix_input_data(self, input_data):
        if input_data is not None:
            if len(input_data) > 0:
                if '\\n' in input_data:
                    # Convert \n in the input into new lines.
                    lines = input_data.split('\\n')
                    input_data = '\n'.join(lines)
            return input_data.split('\n')
        return []
    def _run_send_input(self, session, stdin, input_data):
        if input_data is not None:
            self.info('session.exit_status_ready() %s' % str(session.exit_status_ready()))
            self.info('stdin.channel.closed %s' % str(stdin.channel.closed))
            if stdin.channel.closed is False:
                self.info('sending input data')
                stdin.write(input_data)

    def _run_poll(self, appid, session, timeout, input_data):
        '''
        Poll until the command completes.

        @param session     The session.
        @param timeout     The timeout in seconds.
        @param input_data  The input data.
        @returns the output
        '''
        interval = 0.1
        maxseconds = timeout
        maxcount = maxseconds / interval

        # Poll until completion or timeout
        # Note that we cannot directly use the stdout file descriptor
        # because it stalls at 64K bytes (65536).

        input_idx = 0
        timeout_flag = False

        self.info('polling (%d, %d)' % (maxseconds, maxcount))
        start = datetime.datetime.now()
        start_secs = time.mktime(start.timetuple())
        output = ''
        session.setblocking(0)
        while True:
            if session.recv_ready():
                data = session.recv(self.bufsize)
                output += data
                print(data)
                self.info('read %d bytes, total %d' % (len(data), len(output)))
                if session.send_ready():
                    # We received a potential prompt.
                    # In the future this could be made to work more like
                    # pexpect with pattern matching.
                    if input_idx < len(input_data):
                        data = input_data[input_idx] + '\n'
                        input_idx += 1
                        self.info('sending input data %d' % (len(data)))
                        try:
                            session.send(data)
                        except:
                            fail.append(appid)
            self.info('session.exit_status_ready() = %s' % (str(session.exit_status_ready())))

            if session.exit_status_ready():
                break
            # Timeout check
            now = datetime.datetime.now()
            now_secs = time.mktime(now.timetuple())
            et_secs = now_secs - start_secs
            self.info('timeout check %d %d' % (et_secs, maxseconds))
            #if et_secs > maxseconds:
            #    self.info('polling finished - timeout')
            #    timeout_flag = True
            #    break
            sleep(0.200)
        self.info('polling loop ended')
        # if no fail finish
        if session.recv_ready():
            data = session.recv(self.bufsize)
            output += data
            self.info('read %d bytes, total %d' % (len(data), len(output)))
        self.info('polling finished - %d output bytes' % (len(output)))
        #if no fail finish
        if timeout_flag:
            self.info('appending timeout message')
            print('appending timeout message')
            output += '\nERROR: timeout after %d seconds\n' % (timeout)
            session.close()
        return output

def runCMD(appid, ip, user, passwd, cmd, doLoop = True):

    hostname = ip
    port = 22
    username = user
    password = passwd
    sudo_password = passwd

    # Create the SSH connection
    ssh = MySSH()
    ssh.set_verbosity(False)
    ssh.connect(appid=appid, hostname=hostname, username=username, password=password, port=port)
    if ssh.connected() is False:
        print('ERROR: connection failed. (#' + app + ')')
        fail.append(id)

    def run_cmd(appid, cmd, indata=None, doLoop = True):

        print '=' * 64
        print("starting... app #" + appid)

        print 'command: %s' % (cmd)
        status, output = ssh.run(appid, cmd, indata)
        print 'status : %d' % (status)
        if(status == 0):
            print("finish app #" + appid)
            finish.append(appid)
            app = getApp(conf, appid)
            dep = getDependency(conf, appid)
            if doLoop:
                afterFinish(conf, app, dep)
        else:
            print("fail app #" + appid)
            fail.append(appid)

    if '.py' in cmd:
        pwdd = cmd.split(' ')
        pwdd = pwdd[1].split('/')
        pwdd[len(pwdd)-1] = ''
        pwdd = '/'.join(pwdd)
        run_cmd(appid,'cd ' + pwdd + ' && pwd && '+cmd, sudo_password, doLoop)  # sudo command
    elif '.sh' in cmd:
        run_cmd(appid, cmd, sudo_password, doLoop)  # sudo command
    else:
        run_cmd(appid, cmd, sudo_password, doLoop)  # sudo command

def getApp(conf, app):

    for item in conf["app"]:
        if item["id"] == app:
            return item

def getDependency(conf, app):

    for item in conf["dependency"]:
        if item == app:
            return conf["dependency"][item]

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
def afterFinish(conf, app, dep):

    if dep and "afterFinishStart" in dep:
        afterApp = dep["afterFinishStart"].replace(' ', '').split(',')
        for item in afterApp:
            forStartingApp = getApp(conf, item)
            #check before running or finish ? two running?
            p = multiprocessing.Process(target=runCMD, args=(forStartingApp["id"], forStartingApp["ip"], forStartingApp["user"], forStartingApp["pass"], forStartingApp["cmd"]))
            jobs.append(p)
            jobs_by_id[app["id"]] = p
            p.start()

    if dep and "afterFinishStop" in dep:
        #print(jobs)
        #print(jobs_by_id)
        afterApp = dep["afterFinishStop"].replace(' ', '').split(',')
        for item in afterApp:
            forStopingApp = getApp(conf, item)
            if 'service' in forStopingApp["type"]:
                print("Stoping Service ...#" + item)
                servicehost = forStopingApp["ip"]
                serviceport = forStopingApp["type"].split(':')[1]

                try_ = 1
                sock = connectToSocketServer(servicehost, serviceport)
                while (sock == False and try_ < 3):
                    try_ = try_ + 1
                    sock = connectToSocketServer(servicehost, serviceport)
                    sleep(1)

                if (sock == False):
                    print("can not connect to service " + forStopingApp["ip"] + ":" + forStopingApp["type"].split(':')[1])
                    return

                try:
                    # Send data
                    message = 'getpid'
                    sock.sendall(message)
                    # Look for the response
                    try:
                        pid = sock.recv(4096)
                    except:
                        print('Can not send command')
                        return
                finally:
                    sock.close()

                print('p id of service ' + item + ' is ' + pid)
                cmd = 'kill -9 ' + str(pid)
                p = multiprocessing.Process(target=runCMD,
                                            args=(forStopingApp["id"], app["ip"], app["user"], app["pass"], cmd, False))
                p.start()

            #check before running or finish ? two running?

            """
            if item in jobs_by_id:
                if jobs_by_id[item].is_alive() == True:
                    jobs_by_id[item].terminate()
            """

            """
            filename = forStopingApp["cmd"].split('/')
            filename = filename[len(filename)-1]

            process = subprocess.Popen("ps aux | grep " + filename,
                                       shell=True,
                                       stdout=subprocess.PIPE,
                                       )
            stdout_list = process.communicate()[0].split('\n')
            for line in stdout_list:
                try:
                    fields = line.split()
                    pid = fields[1]
                    print("pid : " + pid)
                    os.kill(int(pid), signal.SIGKILL)
                except:
                    print(" can not kill")
            """
            """
            for item in jobs:
                try:
                    item.terminate()
                except:
                    print("can not kill")
            """
def startApp(conf, appid, dep):
    app = getApp(conf, appid)
    start.append(appid)
    if dep == False:
        p = multiprocessing.Process(target=runCMD, args=(app["id"], app["ip"], app["user"], app["pass"], app["cmd"]))
        jobs.append(p)
        jobs_by_id[app["id"]] = p
        p.start()
    else:
        dep = getDependency(conf, appid)

        if "before" in dep:
            if "beforeType" in dep:
                if dep["beforeType"] == "start":
                    beforeApp = dep["before"].replace(' ','').split(',')
                    beforeFlag = True
                    for item in beforeApp:
                        if item in start and item not in fail and item not in finish:
                            print("")
                        else:
                            beforeFlag = False

                    while beforeFlag == False:
                        print("#" + appid + " wait for start " + dep["before"])
                        sleep(5)

                    if beforeFlag == True:
                        p = multiprocessing.Process(target=runCMD, args=(app["id"], app["ip"], app["user"], app["pass"], app["cmd"]))
                        jobs.append(p)
                        jobs_by_id[app["id"]] = p
                        p.start()

                elif dep["beforeType"] == "finish":
                    beforeApp = dep["before"].replace(' ', '').split(',')
                    beforeFlag = True
                    for item in beforeApp:
                        if item in finish and item not in fail:
                            print("")
                        else:
                            beforeFlag = False

                    while beforeFlag == False:
                        print("#" + appid + " wait for finish " + dep["before"])
                        sleep(5)

                    if beforeFlag == True:
                        p = multiprocessing.Process(target=runCMD, args=(app["id"], app["ip"], app["user"], app["pass"], app["cmd"]))
                        jobs.append(p)
                        jobs_by_id[app["id"]] = p
                        p.start()
    print("finish start app for #" + appid)

def extract_order(input):

    orders = input.replace(' ','').split(',')
    return orders
    """f
    try:
        # Also convert to int since update_time will be string.  When comparing
        # strings, "10" is smaller than "2".
        return int(json['list']['order'])
    except KeyError:
        return 0
    """

def checkExistApp(conf,app):

    for item in conf["app"]:
        if item["id"] == app:
            return True
    return False

def parseConfig(conf):

    if 'app' not in conf:
        sys.exit('parse Error : app key not found')
    if 'dependency' not in conf:
        sys.exit('parse Error : dependency  key not found')
    if 'order' not in conf:
        sys.exit('parse Error : order  key not found')

    for app in conf["app"]:
        if 'id' not in app or 'ip' not in app or 'user' not in app or 'pass' not in app or 'cmd' not in app or 'type' not in app:
            sys.exit('does not complete key in app')
        id = app["id"]
        found = 0
        for _app in conf["app"]:
            if _app["id"] == id :
                found = found + 1
        if found > 1:
            sys.exit('duplicate app id found : app is ' + id)

if __name__ == '__main__':

    rootPath = os.path.realpath(__file__)
    temp = rootPath.split('/')
    rootPath = rootPath.replace(temp[-1],"")
    srvs = ''
    if os.path.isfile(rootPath + 'config/fstarter.config'):
        try:
            conf = json.load(open(rootPath + 'config/fstarter.config'))
        except ValueError, e:
            sys.exit('fstarter config file can not parse!')
    else:
        sys.exit('fstarter config file not found.')

    parseConfig(conf)

    orders = extract_order(conf["order"])

    jobs = []
    jobs_by_id = {}
    start = []
    fail = []
    finish = []

    for app in orders:
        if checkExistApp(conf, app) == False :
            sys.exit(app + ' id not exist in app array')
        if app in conf["dependency"]:
            startApp(conf, app, True)
        else:
            startApp(conf, app, False)

    print("after for")



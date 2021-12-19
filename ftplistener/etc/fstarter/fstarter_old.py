import multiprocessing
from time import sleep
import time
import sys
import os
import os.path
import ConfigParser
import paramiko
import json
import logging
import socket
import datetime

class MySSH:
    def __init__(self, compress=True, verbose=False):
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

    def connect(self, hostname, username, password, port=22):
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
        except socket.error as e:
            self.transport = None
            self.info('failed: %s@%s:%d: %s' % (username, hostname, port, str(e)))
        except paramiko.BadAuthenticationType as e:
            self.transport = None
            self.info('failed: %s@%s:%d: %s' % (username, hostname, port, str(e)))
        return self.transport is not None
    def run(self, cmd, input_data=None, timeout=10):
        self.info('running command: (%d) %s' % (timeout, cmd))
        if self.transport is None:
            self.info('no connection to %s@%s:%s' % (str(self.username), str(self.hostname), str(self.port)))
            return -1, 'ERROR: connection not established\n'

        # Fix the input data.
        input_data = self._run_fix_input_data(input_data)

        # Initialize the session.
        self.info('initializing the session')
        session = self.transport.open_session()
        session.set_combine_stderr(True)
        session.get_pty()
        session.exec_command(cmd)
        output = self._run_poll(session, timeout, input_data)
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

    def _run_poll(self, session, timeout, input_data):   
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
                        session.send(data)
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
        if session.recv_ready():
            data = session.recv(self.bufsize)
            output += data
            self.info('read %d bytes, total %d' % (len(data), len(output)))

        self.info('polling finished - %d output bytes' % (len(output)))
        if timeout_flag:
            self.info('appending timeout message')
            output += '\nERROR: timeout after %d seconds\n' % (timeout)
            session.close() 
        return output

def runCMD(ip, user, passwd, path):

    hostname = ip
    port = 22
    username = user
    password = passwd
    sudo_password = passwd  # assume that it is the same password
    # Create the SSH connection
    ssh = MySSH()
    ssh.set_verbosity(False)
    ssh.connect(hostname=hostname, username=username, password=password, port=port)
    if ssh.connected() is False:
        print 'ERROR: connection failed.'
        sys.exit(1)
    def run_cmd(cmd, indata=None):

        print '=' * 64
        print 'command: %s' % (cmd)
        status, output = ssh.run(cmd, indata)
        print 'status : %d' % (status)
        print 'output : %d bytes' % (len(output))
        print '=' * 64
        print '%s' % (output)

    run_cmd('python ' + path, sudo_password)  # sudo command

def extract_order(json):
    try:
        # Also convert to int since update_time will be string.  When comparing
        # strings, "10" is smaller than "2".
        return int(json['list']['order'])
    except KeyError:
        return 0

if os.path.isfile('fstarter.config'):
    config = ConfigParser.ConfigParser()
    config.readfp(open(r'fstarter.config'))
else:
    sys.exit('fstarter config file not found.')

#  Config
srvs = config.get('Main', 'srvs').strip()

srvs = json.loads(srvs)
srvs = sorted(srvs["list"], key=lambda d: d["order"])
jobs = []
for srv in srvs:
    print('do for connect ' + srv["path"])
    p = multiprocessing.Process(target=runCMD, args=(srv["ip"], srv["user"], srv["pass"], srv["path"]))
    jobs.append(p)
    p.start()

    sleep(2)

import multiprocessing
from time import sleep
import sys
import os
import os.path
import ConfigParser
import paramiko
import json

def runCMD(ip, user, passwd, path):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username=user, password=passwd)
    transport = ssh.get_transport()
    channel_tcpdump = transport.open_session()
    channel_tcpdump.get_pty()
    channel_tcpdump.set_combine_stderr(True)

    command = 'python ' + path
    # channel_tcpdump.exec_command(cmd)  # will return instantly due to new thr$
    _, stdout, _ = ssh.exec_command(command)  # or explicitly pkill tcpdump
    print(stdout.read())  # other command, different shell
    ssh.stat('')
    channel_tcpdump.close()  # close channel and let remote side terminate your$

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


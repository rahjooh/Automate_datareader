import sys
import paramiko
import getpass
from time import sleep

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('10.100.136.68', username='big', password='1')
transport = ssh.get_transport()
channel_tcpdump = transport.open_session()
channel_tcpdump.get_pty()
channel_tcpdump.set_combine_stderr(True)

cmd = "nohup /home/big/ftplistener/1.py >/dev/null 2>&1"        # command will never exit
channel_tcpdump.exec_command(cmd)  # will return instantly due to new thread being spawned.
stdin, stdout, stderr = ssh.exec_command(cmd)
exit_status = stdout.channel.recv_exit_status()
print(exit_status)
#_,stdout,_ = ssh.exec_command(cmd) # or explicitly pkill tcpdump
print stdout.read()     # other command, different shell
channel_tcpdump.close()     # close channel and let remote side terminate your proc.

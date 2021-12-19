
from os.path import isfile, join
import getpass
import paramiko
import socket
import sys
import glob
import md5
import os
import shutil

hostname = '10.100.128.160'
port = 22
username = 'hduser'
password = '1'
dir_local='/home/big/ftplistener/ftproot'
dir_remote = "/home/hduser/data1396"
glob_pattern='*.*'


########################################################################
class SSHConnection(object):
    """"""

    # ----------------------------------------------------------------------
    def __init__(self, host, username, password, port=22):
        """Initialize and setup connection"""
        self.sftp = None
        self.sftp_open = False

        # open SSH Transport stream
        self.transport = paramiko.Transport((host, port))

        self.transport.connect(username=username, password=password)

    # ----------------------------------------------------------------------
    def _openSFTPConnection(self):
        """
        Opens an SFTP connection if not already open
        """
        if not self.sftp_open:
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            self.sftp_open = True

    # ----------------------------------------------------------------------
    def get(self, remote_path, local_path=None):
        """
        Copies a file from the remote host to the local host.
        """
        self._openSFTPConnection()
        self.sftp.get(remote_path, local_path)

        # ----------------------------------------------------------------------

    def put(self, local_path, remote_path=None):
        """
        Copies a file from the local host to the remote host
        """
        self._openSFTPConnection()
        self.sftp.put(local_path, remote_path)

    # ----------------------------------------------------------------------
    def close(self):
        """
        Close SFTP connection and ssh connection
        """
        if self.sftp_open:
            self.sftp.close()
            self.sftp_open = False
        self.transport.close()


if __name__ == "__main__":
    host = "10.100.128.160"
    username = "hduser"
    pw = "1"
    ssh = SSHConnection(host, username, pw)

    for fname in glob.glob(dir_local + os.sep + glob_pattern):
        is_up_to_date = False
        if fname.lower().endswith('zip'):
            local_file = os.path.join(dir_local, fname)
            remote_file = dir_remote + '/' + os.path.basename(fname)
            print 'Copying', local_file, 'to ', remote_file
            ssh.put(local_file, remote_file)
            #shutil.move(local_file, local_file.replace('.zip','.zip_moved'))

    ssh.close()

from os.path import isfile, join
import getpass
import socket
import sys
import glob
import md5
import os
import shutil
from subprocess import Popen,PIPE,STDOUT
from multiprocessing import Process
dir_local = "/home/hduser/data1396/part7-12"
glob_pattern='*.*'

class UNZIPFILE(object):
    def __init__(self,path, filename):
        self.interval = 1
        process = Process(target=self.run, args=(path, filename))
        process.daemon = True
        process.start()
    @classmethod
    def run(self, path, filename):
        y = '1396'
        tmp = filename.split("_")
        dt = tmp[len(tmp) - 1].replace(".zip", "")
        password = y + dt
        password = "isc" + password

        tmp[len(tmp) - 1] = y + tmp[len(tmp) - 1]
        new_file_name = "_".join(tmp)

        destination = new_file_name.replace(".zip", "")
        if(os.path.isdir(destination) == True):
            shutil.rmtree(destination, ignore_errors=True)
        p = Popen(['unzip', '-P', str(password), filename, "-d", destination], stdout=PIPE, stderr=STDOUT)
        output = ''
        for line in p.stdout:
            output = output + str(line)
        if output.find('incorrect password') == True:
            exit()
        else:
            print("unizp " + filename + " finish")

for fname in glob.glob(dir_local + os.sep + glob_pattern):
    if fname.lower().endswith('zip'):
        tmp = fname.split('/')
        filename = tmp[len(tmp) - 1]
        p = UNZIPFILE(fname,filename)
        #print(fname)

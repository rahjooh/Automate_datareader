from time import sleep
import os
import os.path
import re
import glob
import sys
from multiprocessing import Process
from StringIO import StringIO
import urllib2, json,requests
from subprocess import Popen,PIPE,STDOUT
import socket
import json

os.environ['SPARK_HOME'] = "/home/hduser/spark"
sys.path.append("/home/hduser/spark/python")
sys.path.append("/home/hduser/spark/python/lib/py4j-0.10.4-src.zip")

rootPath = os.path.realpath(__file__)
temp = rootPath.split('/')
rootPath = rootPath.replace(temp[-1], "")

import pyspark
from pyspark.sql import SparkSession

conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.master("spark://masternode:7077").appName("Farafekr Runner(ReParquet)").config(conf=conf).getOrCreate()


def sparkStatus():
    url = 'http://masternode:8080/json/'
    try:
        response = urllib2.urlopen(url)
    #except urllib.HTTPError as e:
    #    if e.code == 404:
    #    else:
    except urllib2.URLError as e:
        return False
    else:
        body = response.read()
        data = json.loads(body)
        if data['status'] == 'ALIVE':
            return True
        else:
            return False


class SparkJob():
    def exe(self,foldername):

        mydf = spark.read.parquet("hdfs://10.100.136.60:9000/user/hduser/parquet/" + foldername)
        mydf.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/parquet/" + foldername + "_tmp")
        return True

class REPARQUET(Process):
    def __init__(self, path):
        super(REPARQUET, self).__init__()
        self.path = path
    def run(self):
        if (sparkStatus() == False):
            return 0
        print('Start to reparquet folder ' + self.path)
        obj = SparkJob()
        ret = obj.exe(self.path)
        if(ret == True):
            print('remove old parquet folder ' + self.path + ' ...')

            p = Popen(['/home/hduser/hadoop/bin/hadoop', 'fs', '-rm', '-r', '-skipTrash', '/user/hduser/parquet/' + self.path ], stdout=PIPE, stderr=STDOUT)
            output = ''
            for line in p.stdout:
                output = output + str(line)

            print('out of rename : ' + output)
            if 'Deleted' in output:
                p = Popen(['/home/hduser/hadoop/bin/hadoop', 'fs', '-mv', ('/user/hduser/parquet/' + self.path + '_tmp'),
                           ('/user/hduser/parquet/' + self.path)], stdout=PIPE,
                          stderr=STDOUT)
                output = ''
                for line in p.stdout:
                    output = output + str(line)
            print('out of rename : ' + output)
            return 1
        else:
            print('Can not reparquet folder ' + self.path + ' please try again.')
            return 0

################# MAIN #########################
for k in list(os.environ.keys()):
    if k.lower().endswith('_proxy'):
        del os.environ[k]

if(sparkStatus() == False):
    sys.exit("Please runn spark cluster")

# sort nozoli by default
jobs = []
url = "http://10.100.136.60:50070/webhdfs/v1/user/hduser/parquet?OP=LISTSTATUS"
response = urllib2.urlopen(url)
data = json.loads(response.read())
for file in data["FileStatuses"]["FileStatus"]:
    if "_tmp" not in str(file["pathSuffix"]) and str(file["type"]) == "DIRECTORY":
        while len(jobs) > 1:
            jobs = [job for job in jobs if job.is_alive()]
            print(str(len(jobs)) + ' jobs, wait for free job queue')
            sleep(10)
        print ("Make Reparquet Folder " + str(file["pathSuffix"]))

        p = REPARQUET(str(file["pathSuffix"]))
        p.start()
        p.join()
        jobs.append(p)
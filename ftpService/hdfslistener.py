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

if os.path.isfile('/home/hduser/cando/cando/headers'):
    try:
        headers = json.load(open('/home/hduser/cando/cando/headers'))
    except ValueError, e:
        sys.exit('headers file can not parse!')
else:
    sys.exit('headers file not found.')


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import lit
conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.master("spark://masternode:7077").appName("Farafekr Runner").config(conf=conf).getOrCreate()
masterschema = StructType([StructField("+ ACNO",StringType(),True),StructField("CUSTNO",StringType(),True),StructField("ACCTTYPE",StringType(),True),StructField("ACSUBTYPE",StringType(),True),StructField("STATCD",StringType(),True),StructField("SICCODE",StringType(),True),StructField("ACCOPNOF",StringType(),True),StructField("LSTSTMT",StringType(),True),StructField("STBAL",StringType(),True),StructField("UNCLEAREDFUNDS",StringType(),True),StructField("ACCHEQFLG",StringType(),True),StructField("PSBOOKCOUNT",StringType(),True),StructField("ACCTCNTL",StringType(),True),StructField("BACCNO",StringType(),True),StructField("AMTHOLD",StringType(),True),StructField("EXMTCLOS",StringType(),True),StructField("OPNBR",StringType(),True),StructField("DATEOPN",StringType(),True),StructField("FROZDTE",StringType(),True),StructField("DTEXTACT",StringType(),True),StructField("AMTLSTCR",StringType(),True),StructField("DTLSTCR",StringType(),True),StructField("AMTLSTDR",StringType(),True),StructField("DTLSTDR",StringType(),True),StructField("OPENRES",StringType(),True),StructField("DTCLS",StringType(),True),StructField("CLOSERSN",StringType(),True),StructField("EXTRDAY",StringType(),True),StructField("PREVACSUBYP",StringType(),True),StructField("CHRGFLAG",StringType(),True),StructField("PSBOOKFLAG",StringType(),True),StructField("PreferredDay",StringType(),True),StructField("DepositProfitRate",StringType(),True),StructField("LASTMODIFYDATE",StringType(),True),StructField("LASTMODIFYTIME",StringType(),True),StructField("HISDATE",StringType(),True)])

def detectParquetPath(filename):
    pqPath = 'hdfs://10.100.136.60:9000/user/hduser/parquet'
    masterschema = ''
    for each in headers:
        if each in filename:
            pqPath = pqPath + '/' + each
            return pqPath
    return pqPath

def detectDelimiter(filename):
    for each in headers:
        if each in filename:
            return headers[each]["delimiter"]
    return False

def detectType(filename):
    for each in headers:
        if each in filename:
            if 'type' in headers[each]:
                return headers[each]["type"]
    return False

def detectSchema(filename):
    masterschema = ''
    for each in headers:
        if each in filename:
            masterschema = StructType()
            code = 'masterschema = StructType(['
            for field in headers[each]["headers"].split(","):
                code = code + 'StructField("' + str(field) + '", StringType(), True),'
            if code.endswith(','):
                code = code[:-1]
            code = code + '])'
            exec(code)
            print(each)
            return masterschema
    return False

    """
    if tblName == '':
        return False;

    if 'ACTINFO' in filename:
        masterschema = StructType()
        code = 'masterschema = StructType(['
        for field in headers["ACTINFO"].split(","):
            code = code + 'StructField("'+str(field)+'", StringType(), True),'
        if code.endswith(','):
            code = code[:-1]
        code = code + '])'
        exec(code)
        return masterschema
    elif 'CUSTINFO' in filename:
        masterschema = StructType()
        code = 'masterschema = StructType(['
        for field in headers["CUSTINFO"].split(","):
            code = code + 'StructField("'+str(field)+'", StringType(), True),'
        if code.endswith(','):
            code = code[:-1]
        code = code + '])'
        exec(code)
        return masterschema
    elif 'LASTBAL' in filename:
        masterschema = StructType()
        masterschema = StructType()
        code = 'masterschema = StructType(['
        for field in headers["LASTBAL"].split(","):
            code = code + 'StructField("'+str(field)+'", StringType(), True),'
        if code.endswith(','):
            code = code[:-1]
        code = code + '])'
        exec(code)
        return masterschema
    elif 'STAT' in filename:
        masterschema = StructType()
        code = 'masterschema = StructType(['
        for field in headers["STAT"].split(","):
            code = code + 'StructField("'+str(field)+'", StringType(), True),'
        if code.endswith(','):
            code = code[:-1]
        code = code + '])'
        exec(code)
        return masterschema
    """

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

def thrifserverStatus():
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
        flag = False
        for item in data['activeapps']:
            if 'Thrift' in item['name']:
                flag = True

        if flag:
            return True
        else:
            return False

def thrifserverStart():
    server_address = ('10.100.136.60', 5555)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address)
    except socket.error, e:
        if 'Connection refused' in e:
            return 'Connection refused'
    try:
        message = 'startthriftserver'
        sock.sendall(message)
        try:
            response = sock.recv(4096)
        except:
            return False
    finally:
        sock.close()
    return True

def thrifserverStop():
    server_address = ('10.100.136.60', 5555)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address)
    except socket.error, e:
        if 'Connection refused' in e:
            return 'Connection refused'
    try:
        message = 'stopthriftserver'
        sock.sendall(message)
        try:
            response = sock.recv(4096)
        except:
            return False
    finally:
        sock.close()
    return True

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
    def exe(self,filename,randstr):
        sendMessage(randstr, 'START RUNNER for ' + filename + ' file.')

        masterschema = detectSchema(filename)
        if masterschema == False:
            print("no detect schema")
            return 0
        delimiter = detectDelimiter(filename)
        if delimiter == False:
            print("no detect delimiter")
            return 0

        fileType = detectType(filename)

        pqPath = detectParquetPath(filename)
        sendMessage(randstr, 'path detect is : ' + pqPath)
        sendMessage(randstr, 'file detect is : ' + filename)

        mydf = spark.read.format("com.databricks.spark.csv").schema(masterschema).option("header", "false").option("encoding","cp1256").option("delimiter", delimiter).load("hdfs://10.100.136.60:9000/user/hduser/ftp/" + filename)
        dt = str(filename).split("_")[0]
        sendMessage(randstr, 'append data of ' + dt + ' to parquet DATA.')
        newdf = mydf.withColumn("HISDATE", lit(dt))
        sendMessage(randstr, 'lit finish')
        if fileType == False:
            newdf.write.mode("append").format("parquet").save(pqPath)
        elif fileType.lower() == 'accumulative':
            newdf.write.mode("overwrite").format("parquet").save(pqPath)

        sendMessage(randstr, 'parquet finish')
        return True

class PARQUET(Process):
    def __init__(self, path, randstr):
        super(PARQUET, self).__init__()
        self.path = path
        self.randstr = randstr
    def run(self):
        if (sparkStatus() == False):
            sendMessage(randstr, 'spark not running')
            print('Please runn spark cluster')
            return 0

        print('Start to parquet file ' + self.path)
        obj = SparkJob()
        ret = obj.exe(self.path, self.randstr)
        if(ret == True):
            print('rename file ' + self.path + ' ...')

            p = Popen(['/home/hduser/hadoop/bin/hadoop', 'fs', '-mv', ('/user/hduser/ftp/' + self.path), ('/user/hduser/ftp/' + self.path.replace("_tmp", ""))], stdout=PIPE,
                      stderr=STDOUT)
            output = ''
            for line in p.stdout:
                output = output + str(line)

            """
            fhdfspath = os.path.join(rootPath, 'fhdfs.jar')
            p = Popen(['java', '-jar', fhdfspath, 'renamefile', 'hdfs://10.100.136.60:9000', 'hduser',
                       ('/user/hduser/ftp/' + self.path), ('/user/hduser/ftp/' + self.path.replace("_tmp", ""))],
                      stdout=PIPE, stderr=STDOUT)
            output = ''
            for line in p.stdout:
                output = output + str(line)
            """
            sendMessage(self.randstr, 'out of rename : ' + output)
            print('out of rename : ' + output)
            return 1
        else:
            print('Can not parquet file ' + self.path + ' please try again.')
            return 0

################# MAIN #########################
for k in list(os.environ.keys()):
    if k.lower().endswith('_proxy'):
        del os.environ[k]

if(sparkStatus() == False):
    sys.exit("Please runn spark cluster")

"""
OnStartThrifServerStatus = thrifserverStatus()
if(OnStartThrifServerStatus == True):
    while(thrifserverStatus() == True):
        thrifserverStop()
"""

# sort nozoli by default
jobs = []
url = "http://10.100.136.60:50070/webhdfs/v1/user/hduser/ftp?OP=LISTSTATUS"
response = urllib2.urlopen(url)
data = json.loads(response.read())
for file in data["FileStatuses"]["FileStatus"]:
    if "_tmp" in str(file["pathSuffix"]) and str(file["length"]) != "0":
        while len(jobs) > 1:
            jobs = [job for job in jobs if job.is_alive()]
            print(str(len(jobs)) + ' jobs, wait for free job queue')
            sleep(10)
        print ("detect new file : " + str(file["pathSuffix"]))

        randstr = str(file["pathSuffix"]).split('_')[1]
        sendMessage(randstr, 'start to spark')
        p = PARQUET(str(file["pathSuffix"]), randstr)
        p.start()
        p.join()
        jobs.append(p)

"""
# finish
if(OnStartThrifServerStatus == True):
    while(thrifserverStatus() == False):
        thrifserverStart()

"""

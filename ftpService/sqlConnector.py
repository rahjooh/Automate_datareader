import urllib2, json
import os
import os.path
import sys
import sqlite3
import ConfigParser


os.environ['SPARK_HOME'] = "/home/hduser/spark"
sys.path.append("/home/hduser/spark/python")
sys.path.append("/home/hduser/spark/python/lib/py4j-0.10.4-src.zip")

import pyspark
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import lit
from  pyspark.sql.dataframe import *
from pyspark.sql import SparkSession
conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.master("spark://masternode:7077").appName("Farafekr Runner").config(conf=conf).getOrCreate()


def detectParquetPath(table):
    pqPath = 'hdfs://10.100.136.60:9000/user/hduser/parquet/sql'
    masterschema = ''
    for each in headers:
        if each in table:
            pqPath = pqPath + '/' + each
            return pqPath
    return pqPath

def detectDelimiter(table):
    for each in headers:
        if each in table:
            return headers[each]["delimiter"]
    return False

def detectSchema(table):
    masterschema = ''
    for each in headers:
        if each in table:
            masterschema = StructType()
            code = 'masterschema = StructType(['
            for field in headers[each]["headers"].split(","):
                code = code + 'StructField("' + str(field) + '", StringType(), True),'
            if code.endswith(','):
                code = code[:-1]
            code = code + '])'
            exec(code)
            return masterschema
    return False

if __name__ == '__main__':

    rootPath = os.path.realpath(__file__)
    temp = rootPath.split('/')
    rootPath = rootPath.replace(temp[-1],"")

    if os.path.isfile(rootPath + '/config/sqlConnector.config'):
        config = ConfigParser.ConfigParser()
        config.readfp(open(r'' + rootPath + '/config/sqlConnector.config'))
    else:
        sys.exit('config file not found.')

    if os.path.isfile(rootPath + '/config/sqlConnector.header'):
        try:
            headers = json.load(open(rootPath + '/config/sqlConnector.header'))
        except ValueError, e:
            sys.exit('headers file can not parse!')
    else:
        sys.exit('headers file not found.')

    host = config.get('sql', 'host')
    port = config.get('sql', 'port')
    db = config.get('sql', 'db')
    username = config.get('sql', 'username')
    password = config.get('sql', 'password')
    table = config.get('sql', 'table')

    if not os.path.exists(rootPath + 'sqllog'):
        os.makedirs(rootPath + 'sqllog')

    dt = ""
    if os.path.isfile(rootPath + '/sqllog/' + table):
        with open(rootPath + '/sqllog/' + table, 'r') as content_file:
            dt = content_file.read()
            dt = dt.strip()

    masterschema = detectSchema(table)
    if masterschema == False:
        print("no detect schema")
        sys.exit()
    delimiter = detectDelimiter(table)
    if delimiter == False:
        print("no detect delimiter")
        sys.exit()

    pqPath = detectParquetPath(table)

    #hamrahCardschema = StructType([StructField("ID",StringType(),True),StructField("CardNo",StringType(),True),StructField("AMOUNT",StringType(),True),StructField("MOBILENO",StringType(),True),StructField("STATUS",StringType(),True),StructField("PERSIANCREATIONDAT",StringType(),True),StructField("PERSIANCREATIONTIME",StringType(),True),StructField("REQUESTID",StringType(),True),StructField("REQUESTTYPE",StringType(),True),StructField("HISDATE",StringType(),True),StructField("CUSTNO",StringType(),True)])
    card = spark.createDataFrame(sc.emptyRDD(), masterschema)
    if dt == '':
        mydf = spark.read.jdbc(
            url="jdbc:sqlserver://" + host + ":" + port + ";databaseName=" + db + ";user=" + username + ";password=" + password, table=table)
        maxrow = mydf.agg({"Hisdate": "max"}).collect()[0]
        maxHisdate = maxrow['max(Hisdate)']
        print(maxHisdate)
        f = open(rootPath + '/sqllog/' + table, "w+")
        f.write(str(maxHisdate))
        f.close()

        card = card.union(mydf)
        card.write.format("parquet").mode("append").save(pqPath)
    else:
        conditions = ["Hisdate > " + str(dt)]
        mydf = spark.read.jdbc(url="jdbc:sqlserver://" + host + ":" + port + ";databaseName=" + db + ";user=" + username + ";password=" + password ,table=table,predicates = conditions)
        maxrow = mydf.agg({"Hisdate": "max"}).collect()[0]
        maxHisdate = maxrow['max(Hisdate)']
        print(maxHisdate)
        f = open(rootPath + '/sqllog/' + table, "w+")
        f.write(str(maxHisdate))
        f.close()
        card = card.union(mydf)
        card.write.format("parquet").mode("append").save(pqPath)



            
    







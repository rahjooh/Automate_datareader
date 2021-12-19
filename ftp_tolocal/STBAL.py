import os,sys
os.environ['SPARK_HOME'] = "/home/hduser/spark"
sys.path.append("/home/hduser/spark/python")
sys.path.append("/home/hduser/spark/python/lib/py4j-0.10.4-src.zip")
import pyspark
from pyspark.sql import SparkSession
import time
import pandas as pd 
import json
from pyspark.sql.types import StructType,StructField,StringType,BooleanType
from pyspark.sql.functions import lit
from  pyspark.sql.dataframe import *
conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.master("spark://masternode:7077").appName("Arashi's Importer").config(conf=conf).getOrCreate()
import time
print("Not Yet")
time.sleep(18000)


Statschema=StructType([StructField("ID",StringType(),True),StructField("BRANCH",StringType(),True),StructField("CUSTNO",StringType(),True),StructField("ACNO",StringType(),True),StructField("TRANDATE",StringType(),True),StructField("TRANTIME",StringType(),True),StructField("TERMINALCODE",StringType(),True),StructField("TERMINALNUM",StringType(),True),StructField("TRANAMOUNT",StringType(),True),StructField("TRANREMAININGAMOUNT",StringType(),True),StructField("TRANSTATE",StringType(),True),StructField("TRANCODE",StringType(),True),StructField("TRANBRANCH",StringType(),True),StructField("STMTCODE",StringType(),True),StructField("TRANDESC",StringType(),True),StructField("TRANDESC1",StringType(),True),StructField("HISDATE",StringType(),True),StructField("PATH",StringType(),True),StructField("LATINTRANDATE",StringType(),True),])
rdd = spark.createDataFrame(sc.emptyRDD(), Statschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table="(select * from STAT where [TRAN-DATE] between '980401' and '980431') as TEMP " )
rdd = rdd.union(mydf)
rdd.write.format("parquet").save("hdfs://10.100.136.60:9000/user/hduser/StatTemp")



Lastbalschema = StructType([StructField("ID",StringType(),True),StructField("CREATETRANSDATE",StringType(),True),StructField("ACNO",StringType(),True),StructField("CUSTNO",StringType(),True),StructField("REMAININGAMOUNTCURRENT",StringType(),True),StructField("REMAININGAMOUNTUSE",StringType(),True),StructField("REMAININGAMOUNTEFFECTIVE",StringType(),True),StructField("BALDATE",StringType(),True),StructField("BALTIME",StringType(),True),StructField("HISDATE",StringType(),True),StructField("PATH",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), Lastbalschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table="(select * from LASTBAL where [HisDate] between '980401' and '980431') as TEMP " )
rdd = rdd.union(mydf)
rdd.write.format("parquet").save("hdfs://10.100.136.60:9000/user/hduser/LastTemp")


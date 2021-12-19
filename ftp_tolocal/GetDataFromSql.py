import os,sys
os.environ['SPARK_HOME'] = "/home/hduser/spark"
sys.path.append("/home/hduser/spark/python")
sys.path.append("/home/hduser/spark/python/lib/py4j-0.10.4-src.zip")
import pyspark
from pyspark.sql import SparkSession
import time
import urllib2, json
from pyspark.sql.types import StructType,StructField,StringType,BooleanType
from pyspark.sql.functions import lit
from  pyspark.sql.dataframe import *
conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.master("spark://masternode:7077").appName("Arashi's Importer").config(conf=conf).getOrCreate()

#Actinfo
  
oldschema = StructType([StructField("ID",StringType(),True),StructField("ACNO",StringType(),True),StructField("CUSTNO",StringType(),True),StructField("ACCTTYPE",StringType(),True),StructField("ACSUBTYPE",StringType(),True),StructField("STATCD",StringType(),True),StructField("SICCODE",StringType(),True),StructField("ACCOPNOF",StringType(),True),StructField("LSTSTMT",StringType(),True),StructField("STBAL",StringType(),True),StructField("UNCLEAREDFUNDS",StringType(),True),StructField("ACCHEQFLG",StringType(),True),StructField("PSBOOKCOUNT",StringType(),True),StructField("ACCTCNTL",StringType(),True),StructField("BACCNO",StringType(),True),StructField("AMTHOLD",StringType(),True),StructField("EXMTCLOS",StringType(),True),StructField("OPNBR",StringType(),True),StructField("DATEOPN",StringType(),True),StructField("FROZDTE",StringType(),True),StructField("DTEXTACT",StringType(),True),StructField("AMTLSTCR",StringType(),True),StructField("DTLSTCR",StringType(),True),StructField("AMTLSTDR",StringType(),True),StructField("DTLSTDR",StringType(),True),StructField("OPENRES",StringType(),True),StructField("DTCLS",StringType(),True),StructField("CLOSERSN",StringType(),True),StructField("EXTRDAY",StringType(),True),StructField("PREVACSUBYP",StringType(),True),StructField("CHRGFLAG",StringType(),True),StructField("PSBOOKFLAG",StringType(),True),StructField("PreferredDay",StringType(),True),StructField("DepositProfitRate",StringType(),True),StructField("LASTMODIFYDATE",StringType(),True),StructField("LASTMODIFYTIME",StringType(),True),StructField("HisDate",StringType(),True),])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table="ACTINFO")
rdd = rdd.union(mydf)
rdd.select("ACNO","CUSTNO","HisDate").write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Actinfo")
print("Actinfo has been written .")

#Custinfo :

oldschema = StructType([StructField("ID",StringType(),True),StructField("CUSTNO",StringType(),True),StructField("CUSTOPNOF",StringType(),True),StructField("CUSTYPE",StringType(),True),StructField("BRANCH",StringType(),True),StructField("DTBRTH",StringType(),True),StructField("MARSTAT",StringType(),True),StructField("DATEOPN",StringType(),True),StructField("NATCD",StringType(),True),StructField("RESTAT",StringType(),True),StructField("ISUCAP",StringType(),True),StructField("ESTPDTE",StringType(),True),StructField("COMPGNO",StringType(),True),StructField("IDNUMBER",StringType(),True),StructField("CUSUBTYP",StringType(),True),StructField("CUSTSAL",StringType(),True),StructField("DUPL-FLAG",StringType(),True),StructField("OCCUPCD",StringType(),True),StructField("SEXCODE",StringType(),True),StructField("SHRTNAM",StringType(),True),StructField("CUSENGN",StringType(),True),StructField("ARBNME",StringType(),True),StructField("MODIFDTE",StringType(),True),StructField("MODITIME",StringType(),True),StructField("PROFCODE",StringType(),True),StructField("FATHERNAME",StringType(),True),StructField("ISSUEDTOWN",StringType(),True),StructField("SDRCODE",StringType(),True),StructField("ISSUEDDATE",StringType(),True),StructField("REGIONCODE",StringType(),True),StructField("ECONOMICCODE",StringType(),True),StructField("FAXNUMBER",StringType(),True),StructField("EDUCLVL",StringType(),True),StructField("NOOFPRTNR",StringType(),True),StructField("IDNUMBERCORP",StringType(),True),StructField("ADDRS1",StringType(),True),StructField("ADDRS2",StringType(),True),StructField("ZIPCODE",StringType(),True),StructField("TELNO",StringType(),True),StructField("HisDate",StringType(),True),StructField("PATH",StringType(),True),StructField("DTBRTH8",StringType(),True),StructField("CUSTOPFFICER",StringType(),True),StructField("IDENTDOCSTYPE",StringType(),True),StructField("STMTSENDTYPE",StringType(),True),StructField("ECONOMICSECTOR",StringType(),True),StructField("OLDBRANCH",StringType(),True),StructField("WSSHAHABCODE",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table="CUSTINFO")
rdd = rdd.union(mydf)
rdd.select("CUSTNO","ECONOMICCODE","HisDate").write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Custinfo")
print("Custinfo has been written .")


#Card-Debit : 

oldschema = StructType([StructField("ID",StringType(),True),StructField("BRANCH",StringType(),True),StructField("CARDNO",StringType(),True),StructField("ACNO",StringType(),True),StructField("MELINO",StringType(),True),StructField("CUSTNO",StringType(),True),StructField("STATUS",StringType(),True),StructField("SODURDATE",StringType(),True),StructField("ASLIFAREE",StringType(),True),StructField("HisDate",StringType(),True),StructField("PATH",StringType(),True),StructField("EXPDATE",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table='[Card-Debit]')
rdd = rdd.union(mydf)
rdd.select("CARDNO","ACNO","CUSTNO","BRANCH","HisDate").write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/CardDebit")
print("Debit Card has been written .")

#Card-Bon : 

oldschema = StructType([StructField("ID",StringType(),True),StructField("BRANCH",StringType(),True),StructField("CARDNO",StringType(),True),StructField("ACNO",StringType(),True),StructField("MELINO",StringType(),True),StructField("CUSTNO",StringType(),True),StructField("REMAININGAMOUNT",StringType(),True),StructField("REMAININGAMOUNT2",StringType(),True),StructField("STATUS",StringType(),True),StructField("HisDate",StringType(),True),StructField("PATH",StringType(),True),StructField("ISSUEDATE",StringType(),True),StructField("ACTIVATIONDATE",StringType(),True),StructField("EXPDATE",StringType(),True),StructField("CARDNAME",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table='[Card-Bon]')
rdd = rdd.union(mydf)
rdd.select("CARDNO","ACNO","CUSTNO","BRANCH","HisDate").write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/CardBon")
print("Bon Card has been written .")


#Card-Gift : 

oldschema = StructType([StructField("ID",StringType(),True),StructField("BRANCH",StringType(),True),StructField("ACNO",StringType(),True),StructField("STATUS",StringType(),True),StructField("CARDNO",StringType(),True),StructField("REMAININGAMOUNT",StringType(),True),StructField("REMAININGAMOUNT2",StringType(),True),StructField("HisDate",StringType(),True),StructField("PATH",StringType(),True),StructField("ISSUEDATE",StringType(),True),StructField("ACTIVATIONDATE",StringType(),True),StructField("EXPDATE",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table='[Card-Gift]')
rdd = rdd.union(mydf)
rdd.select("CARDNO","ACNO","BRANCH","HisDate").write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/CardGift")
print("Gift Card has been written .")

#Chakavak-Report : 

oldschema =  StructType([StructField("BenefName",StringType(),True),StructField("BenefSheba",StringType(),True),StructField("BenefBrCode",StringType(),True),StructField("OrderBankName",StringType(),True),StructField("Amount",StringType(),True),StructField("HisDate",StringType(),True),StructField("BenefBankCode",StringType(),True),StructField("ChqFinalStat",StringType(),True),StructField("BenefMeliCode",StringType(),True),StructField("ChqType",StringType(),True),StructField("OrderBankBranch",StringType(),True),StructField("OrderTrackNo",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table='VW_Chakavakreport_BigData')
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Chakavak")
print("Chakavak has been written .")

#Satna : 

oldschema =  StructType([StructField("BeneficiaryCommentShaba",StringType(),True),StructField("OrderOfSwiftCode",StringType(),True),StructField("Amount",StringType(),True),StructField("HisDate",StringType(),True),StructField("BeneficiarySwiftCode",StringType(),True),StructField("BeneficiaryCommentAcno",StringType(),True),StructField("OrderOfCommentAcno",StringType(),True),StructField("TransactionType",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table='VW_satna_BigData')
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Satna")
print("Satna has been written .")

#SHP_RTGS :


oldschema = StructType([StructField("PspCode",StringType(),True),StructField("IBAN",StringType(),True),StructField("AmountShaparak",StringType(),True),StructField("HisDate",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table='VW_SHP_RTGS_BigData')
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/SHPRTGS")
print("SHP_RTGS has been written .")

#Total_TXN :

oldschema = StructType([StructField("CardNo",StringType(),True),StructField("Amount",StringType(),True),StructField("FinancialDate",StringType(),True),StructField("AcquireBankCode",StringType(),True),StructField("Branch",StringType(),True),StructField("ProcessCode",StringType(),True),StructField("TerminalTypeCode",StringType(),True),StructField("SuccessOrFailure",StringType(),True),StructField("LocalOrShetab",StringType(),True),StructField("DestPan",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table='VW_Total_Txn_BigData')
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/TotalTxn")
print("Total_Txn has been written .")


#BLL_Chakavak :

oldschema = StructType([StructField("TranAtmX",StringType(),True),StructField("Acc",StringType(),True),StructField("OptionInfo1",StringType(),True),StructField("TranDate",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table='VW_BLL_CHAKAVAK_BigData')
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/BLLCHAKAVAK")
print("BLL Chakavak has been written .")


#IncommingRegularTransactions : 

oldschema = StructType([StructField("RecAccNo",StringType(),True),StructField("SenderBank",StringType(),True),StructField("Amount",StringType(),True),StructField("TrackID",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table='VW_IncommingRegularTransactions_BigData')
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Incomming")
print("IncommingRegularTransactions has been written .")


#OutgoingTransactions : 

oldschema = StructType([StructField("SenderAccNo",StringType(),True),StructField("SenderBank",StringType(),True),StructField("Amount",StringType(),True),StructField("TraceId",StringType(),True),StructField("HisDate",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table='VW_OutgoingTransactions_BigData')
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/Outgoing")
print("OutgoingTransactions has been written .")


#BasicInfo : 

oldschema = StructType([StructField("ID",StringType(),True),StructField("TableName",StringType(),True),StructField("FieldName",StringType(),True),StructField("ValueCode",StringType(),True),StructField("Description",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table='VW_BasicInfoMetaData')
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/IncomeOutgo/BasicInfo")
print("BasicInfo has been written .")




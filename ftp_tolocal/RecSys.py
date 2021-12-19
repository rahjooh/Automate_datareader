import os,sys
os.environ['SPARK_HOME'] = "/home/hduser/spark"
sys.path.append("/home/hduser/spark/python")
sys.path.append("/home/hduser/spark/python/lib/py4j-0.10.4-src.zip")
import pyspark
from pyspark.sql import SparkSession
import time
import json
from pyspark.sql.types import StructType,StructField,StringType,BooleanType
from pyspark.sql.functions import lit
from  pyspark.sql.dataframe import *
conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.master("spark://masternode:7077").appName("Arashi's Importer").config(conf=conf).getOrCreate()
"""
time.sleep(18000)

#Actinfo_Historical
oldschema = StructType([StructField("ID",StringType(),True),StructField("ACNO",StringType(),True),StructField("CUSTNO",StringType(),True),StructField("ACCTTYPE",StringType(),True),StructField("ACSUBTYPE",StringType(),True),StructField("STATCD",StringType(),True),StructField("SICCODE",StringType(),True),StructField("ACCOPNOF",StringType(),True),StructField("LSTSTMT",StringType(),True),StructField("STBAL",StringType(),True),StructField("UNCLEAREDFUNDS",StringType(),True),StructField("ACCHEQFLG",StringType(),True),StructField("PSBOOKCOUNT",StringType(),True),StructField("ACCTCNTL",StringType(),True),StructField("BACCNO",StringType(),True),StructField("AMTHOLD",StringType(),True),StructField("EXMTCLOS",StringType(),True),StructField("OPNBR",StringType(),True),StructField("DATEOPN",StringType(),True),StructField("FROZDTE",StringType(),True),StructField("DTEXTACT",StringType(),True),StructField("AMTLSTCR",StringType(),True),StructField("DTLSTCR",StringType(),True),StructField("AMTLSTDR",StringType(),True),StructField("DTLSTDR",StringType(),True),StructField("OPENRES",StringType(),True),StructField("DTCLS",StringType(),True),StructField("CLOSERSN",StringType(),True),StructField("EXTRDAY",StringType(),True),StructField("PREVACSUBYP",StringType(),True),StructField("CHRGFLAG",StringType(),True),StructField("PSBOOKFLAG",StringType(),True),StructField("PreferredDay",StringType(),True),StructField("DepositProfitRate",StringType(),True),StructField("LASTMODIFYDATE",StringType(),True),StructField("LASTMODIFYTIME",StringType(),True),StructField("HisDate",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table="(select * from [ACTINFO_Historical] with (nolock) where [HisDate] between '980301' and '980331') as Tempa")
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/RecSys/Actinfo")
print("Actinfo has been written .")

#Custinfo :

oldschema = StructType([StructField("ID",StringType(),True),StructField("CUSTNO",StringType(),True),StructField("CUSTOPNOF",StringType(),True),StructField("CUSTYPE",StringType(),True),StructField("BRANCH",StringType(),True),StructField("DTBRTH",StringType(),True),StructField("MARSTAT",StringType(),True),StructField("DATEOPN",StringType(),True),StructField("NATCD",StringType(),True),StructField("RESTAT",StringType(),True),StructField("ISUCAP",StringType(),True),StructField("ESTPDTE",StringType(),True),StructField("COMPGNO",StringType(),True),StructField("IDNUMBER",StringType(),True),StructField("CUSUBTYP",StringType(),True),StructField("CUSTSAL",StringType(),True),StructField("DUPL-FLAG",StringType(),True),StructField("OCCUPCD",StringType(),True),StructField("SEXCODE",StringType(),True),StructField("SHRTNAM",StringType(),True),StructField("CUSENGN",StringType(),True),StructField("ARBNME",StringType(),True),StructField("MODIFDTE",StringType(),True),StructField("MODITIME",StringType(),True),StructField("PROFCODE",StringType(),True),StructField("FATHERNAME",StringType(),True),StructField("ISSUEDTOWN",StringType(),True),StructField("SDRCODE",StringType(),True),StructField("ISSUEDDATE",StringType(),True),StructField("REGIONCODE",StringType(),True),StructField("ECONOMICCODE",StringType(),True),StructField("FAXNUMBER",StringType(),True),StructField("EDUCLVL",StringType(),True),StructField("NOOFPRTNR",StringType(),True),StructField("IDNUMBERCORP",StringType(),True),StructField("ADDRS1",StringType(),True),StructField("ADDRS2",StringType(),True),StructField("ZIPCODE",StringType(),True),StructField("TELNO",StringType(),True),StructField("HisDate",StringType(),True),StructField("PATH",StringType(),True),StructField("DTBRTH8",StringType(),True),StructField("CUSTOPFFICER",StringType(),True),StructField("IDENTDOCSTYPE",StringType(),True),StructField("STMTSENDTYPE",StringType(),True),StructField("ECONOMICSECTOR",StringType(),True),StructField("OLDBRANCH",StringType(),True),StructField("WSSHAHABCODE",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table="CUSTINFO")
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/RecSys/Custinfo")
print("Custinfo has been written .")


#Card-Bon :
"""
oldschema = StructType([StructField("ID",StringType(),True),StructField("BRANCH",StringType(),True),StructField("CARDNO",StringType(),True),StructField("ACNO",StringType(),True),StructField("MELINO",StringType(),True),StructField("CUSTNO",StringType(),True),StructField("REMAININGAMOUNT",StringType(),True),StructField("REMAININGAMOUNT2",StringType(),True),StructField("STATUS",StringType(),True),StructField("HisDate",StringType(),True),StructField("PATH",StringType(),True),StructField("ISSUEDATE",StringType(),True),StructField("ACTIVATIONDATE",StringType(),True),StructField("EXPDATE",StringType(),True),StructField("CARDNAME",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table="(select * from [Card-BonHist] with (nolock) where [HisDate] between '980301' and '980331') as TEMP ")
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/RecSys/CardBon")
print("Bon Card has been written .")
"""
#Total_TXN :

oldschema = StructType([StructField("ID",StringType(),True),StructField("FinancialDate",StringType(),True),StructField("LocalTime",StringType(),True),StructField("LocalDate",StringType(),True),StructField("InTime",StringType(),True),StructField("OutTime",StringType(),True),StructField("TraceNo",StringType(),True),StructField("Amount",StringType(),True),StructField("AcquireCurrency",StringType(),True),StructField("CardNo",StringType(),True),StructField("AcquireBankCode",StringType(),True),StructField("IssuerBankCode",StringType(),True),StructField("LocalOrShetab",StringType(),True),StructField("FromAccountNo",StringType(),True),StructField("ToAccountNo",StringType(),True),StructField("ProcessCode",StringType(),True),StructField("Branch",StringType(),True),StructField("MerchantNumber",StringType(),True),StructField("OnlineOROffline",StringType(),True),StructField("TerminalTypeCode",StringType(),True),StructField("TerminalNo",StringType(),True),StructField("ResponseCode",StringType(),True),StructField("ReturnCode",BooleanType(),True),StructField("MessageType",StringType(),True),StructField("StatusCode",StringType(),True),StructField("SuccessOfFailure",StringType(),True),StructField("ReferenceNo",StringType(),True),StructField("TransactionAmount",StringType(),True),StructField("IssuerCurrency",StringType(),True),StructField("SettlementDateInIssuerSwitch",StringType(),True),StructField("RevisoryAmount",StringType(),True),StructField("ExpireDate",StringType(),True),StructField("SourceOrganization",StringType(),True),StructField("TraceNoForTransactionsWithREVERSAL",StringType(),True),StructField("DateForTransactionWithREVERSAL",StringType(),True),StructField("TimeForTransactionWithREVERSAL",StringType(),True),StructField("AcquireForTransactionWithREVERSAL",StringType(),True),StructField("IssuerForTransactionWithREVERSAL",StringType(),True),StructField("ShetabFee",StringType(),True),StructField("NetworkFee",StringType(),True),StructField("AcquireFee",StringType(),True),StructField("AccountIssuerBranch",StringType(),True),StructField("InDate",StringType(),True),StructField("PrivateUse1",StringType(),True),StructField("BillNumber",StringType(),True),StructField("PaymentNumber",StringType(),True),StructField("BillTypeAndOrganizationCode",StringType(),True),StructField("OperationNo",StringType(),True),StructField("Wage",StringType(),True),StructField("DESTPAN",StringType(),True),StructField("MERCHNO",StringType(),True),StructField("HisDate",StringType(),True),StructField("path",StringType(),True),StructField("PrivateUse2",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), oldschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table="(select * from [Total_Txn] with (nolock) where [Financial Date] between '1398/03/01' and '1398/03/31') as Tempr")
rdd = rdd.union(mydf)
rdd.write.format("parquet").mode("overwrite").save("hdfs://10.100.136.60:9000/user/hduser/RecSys/TotalTxn")
print("Total_Txn has been written .")

#LastBal
Lastbalschema = StructType([StructField("ID",StringType(),True),StructField("CREATETRANSDATE",StringType(),True),StructField("ACNO",StringType(),True),StructField("CUSTNO",StringType(),True),StructField("REMAININGAMOUNTCURRENT",StringType(),True),StructField("REMAININGAMOUNTUSE",StringType(),True),StructField("REMAININGAMOUNTEFFECTIVE",StringType(),True),StructField("BALDATE",StringType(),True),StructField("BALTIME",StringType(),True),StructField("HISDATE",StringType(),True),StructField("PATH",StringType(),True)])
rdd = spark.createDataFrame(sc.emptyRDD(), Lastbalschema)
mydf = spark.read.jdbc(url="jdbc:sqlserver://10.100.120.17:1433;databaseName=tat_dwbi_ods;user=CandoPKG;password=F#i8uUyh6Ty5faR",table="(select * from [LASTBAL] with (nolock) where [HisDate] between '980301' and '980331') as TEMP " )
rdd = rdd.union(mydf)
rdd.write.format("parquet").save("hdfs://10.100.136.60:9000/user/hduser/RecSys/Lastbal")
print("Lastbal has been written .")
"""



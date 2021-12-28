import os
import sys
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
import pyspark
import random
import operator
from pyspark.sql import SparkSession
from datetime import datetime,timedelta, date
from pyspark.mllib.regression import LabeledPoint
import os
import re
import pyspark
import random
import operator
from pyspark.sql import SparkSession
from datetime import date,timedelta
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from tempfile import NamedTemporaryFile
from fileinput import input
from glob import glob

HADOOP_HOME = "./resources/hadoop_home"
JDBC_JAR = "./resources/postgresql-42.2.8.jar"
PYSPARK_PYTHON = "python3"
PYSPARK_DRIVER_PYTHON = "python3"

os.environ["HADOOP_HOME"] = HADOOP_HOME
sys.path.append(HADOOP_HOME + "\\bin")
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_DRIVER_PYTHON
conf = SparkConf()  # create the configuration
conf.set("spark.jars", JDBC_JAR)
spark = SparkSession.builder \
    .config(conf=conf) \
    .master("local") \
    .appName("Training") \
    .getOrCreate()

sc = pyspark.SparkContext.getOrCreate()
aircraft = "XY-FFT"
date = "010615"

g_username = "gonzalo.cordova"
g_password = "DB060601"
m_username = "miquel.palet.lopez"
m_password = "DB070501"

sess = SparkSession(sc)
    
rx = date + "-...-...-....-" + aircraft + ".csv"
#model = DecisionTreeModel.load(sc, "myDecisionTreeClassificationModel")
target_files = []

for root, dirs, files in os.walk("./resources/trainingData/"):
    for file in files:
        res = re.match(rx, file)
        if res:
            target_files.append(file)
    
DW = (sess.read
	.format("jdbc")
	.option("driver","org.postgresql.Driver")
	.option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/DW?sslmode=require")
	.option("dbtable", "public.aircraftutilization")
	.option("user", g_username)
	.option("password", g_password)
	.load())
    
KPIs = (DW
    .select("aircraftid","timeid","flighthours","flightcycles","delayedminutes")
    .rdd
    .map(lambda t: ((t[1],t[0]),(float(t[2]),int(t[3]),int(t[4]))))
    .sortByKey())
    
print(target_files[0])
    
CSVfile = (sc.wholeTextFiles("./resources/trainingData/" + target_files[0]))
    #.map(lambda t: ((date(2000+int(t[0].split("/")[-1][4:6]),int(t[0].split("/")[-1][2:4]),int(t[0].split("/")[-1][0:2])),t[0].split("/")[-1][20:26]),list(t[1].split("\n")))))
for i in CSVfile.collect():
    print(i)

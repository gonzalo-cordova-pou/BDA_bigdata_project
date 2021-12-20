import os
import sys
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

import management
import analysis
import runtime

HADOOP_HOME = "./resources/hadoop_home"
JDBC_JAR = "./resources/postgresql-42.2.8.jar"
PYSPARK_PYTHON = "python3"
PYSPARK_DRIVER_PYTHON = "python3"

if(__name__== "__main__"):
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

    if(len(sys.argv) < 2):
        print("Wrong pipeline, usage: (management, analysis, runtime)")
        exit()
    if(sys.argv[1] == "management"):
        print("#########  MANAGEMENT  ##########")
        management.process(sc)
    elif(sys.argv[1] == "analysis"):
        print("#########  ANALYSIS  ##########")
        analysis.process(sc)
    elif(sys.argv[1] == "runtime"):
        aircraft = sys.argv[2]
        date = sys.argv[3]
        print("#########  RUNTIME  ##########")
        runtime.process(sc, aircraft, date)
    else:
       print("Wrong pipeline name (management, analysis, runtime)")

    #Create and point to your pipelines here

from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
import pyspark
import random
import operator
from pyspark.sql import SparkSession
from datetime import timedelta, date
from pyspark.mllib.regression import LabeledPoint
import os
import re
import operator
from tempfile import NamedTemporaryFile
from fileinput import input
from glob import glob



g_username = "gonzalo.cordova"
g_password = "DB060601"
m_username = "miquel.palet.lopez"
m_password = "DB070501"

def process(sc, aircraft, date_):
    sess = SparkSession(sc)
    
    rx = date_ + "-...-...-....-" + aircraft + ".csv"
    model = DecisionTreeModel.load(sc, "myDecisionTreeClassificationModel")
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
    
    CSVfile = (sc.wholeTextFiles("./resources/trainingData/" + target_files[0])
        .map(lambda t: ((date(2000+int(t[0].split("/")[-1][4:6]),int(t[0].split("/")[-1][2:4]),int(t[0].split("/")[-1][0:2])),t[0].split("/")[-1][20:26]),list(t[1].split("\n"))))
        .flatMap(lambda t: [(t[0], value) for value in t[1]])
        .filter(lambda t: "value" not in t[1])
        .filter(lambda t: t[1] != '')
        .mapValues(lambda t: (float(t.split(";")[-1]),1))
        .reduceByKey(lambda t1,t2: (t1[0]+t2[0],t1[1]+t2[1]))
        .mapValues(lambda t: t[0]/t[1])
        .sortByKey())

    
    rdd = (CSVfile
		.join(KPIs))
    
    t =  rdd.collect()[0]

    sample = [t[1][0],t[1][1][0],t[1][1][1],t[1][1][2]]
    print("\n \n")
    print("Sample:  ", sample)
    prediction = model.predict(sample)
    if (prediction == 0):
        print("predicted result:   NO MAINTENANCE")
        print("\n \n")
        print("**Miquel Palet and Gonzalo Cordova are not responsible for the problems (or even deaths) that the bad perforance of this model may cause**")
    else:
        print("predicted result: MAINTENANCE")
        print("\n \n")
        print("**Miquel Palet and Gonzalo Cordova are not responsible for the problems (or even deaths) that the bad perforance of this model may cause**")
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
import pyspark
import random
import operator
from pyspark.sql import SparkSession
from datetime import datetime,timedelta
from pyspark.mllib.regression import LabeledPoint
import os
import re

g_username = "gonzalo.cordova"
g_password = "DB060601"
m_username = "miquel.palet.lopez"
m_password = "DB070501"

def process(sc, aircraft, date):
    sess = SparkSession(sc)
    
    date = "010615"
    aircraft = "XY-FFT"
    
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
        .filter(lambda t: t[0] == aircraft)
		.map(lambda t: ((t[1].strftime('%Y-%m-%d'),t[0]),(float(t[2]),int(t[3]),int(t[4]))))
		.sortByKey())
    
    CSVfile = (sc.wholeTextFiles("./resources/trainingData/" + target_files[0])
        .map(lambda t: ((datetime.strptime(t[0].split("/")[-1][0:6],'%d%m%y').strftime('%Y-%m-%d'),t[0].split("/")[-1][20:26]),list(t[1].split("\n"))))
		.flatMap(lambda t: [(t[0], value) for value in t[1]])
		.filter(lambda t: "value" not in t[1])
		.filter(lambda t: t[1] != '')
		.mapValues(lambda t: (float(t.split(";")[-1]),1))
		.reduceByKey(lambda t1,t2: (t1[0]+t2[0],t1[1]+t2[1]))
		.mapValues(lambda t: t[0]/t[1])
		.sortByKey())
    
    rdd = (CSVfile
		.join(KPIs))
    
    for i in rdd.collect():
        print(i)


    # PARAMETERS FOR model.predict
    #  ---------------------------------------
    #    X  ->  {array-like, sparse matrix} of shape (n_samples, n_features)
    #              The input samples. Internally, it will be converted to dtype=np.float32 and if a sparse matrix is provided to a sparse csr_matrix.
    # check_input ->  bool, default=True
    #        Allow to bypass several input checking. Don’t use this parameter unless you know what you do.

    # Returns
    #    y  ->  array-like of shape (n_samples,) or (n_samples, n_outputs)
    #              The predicted classes, or the predict values.

    # Evaluate model on test instances and compute test error


    #predictions = model.predict(inputData.map(lambda x: x.features))
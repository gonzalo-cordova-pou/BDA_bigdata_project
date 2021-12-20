import pyspark
import random
import operator
from pyspark.sql import SparkSession
from datetime import date,timedelta
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint

g_username = "gonzalo.cordova"
g_password = "DB060601"
m_username = "miquel.palet.lopez"
m_password = "DB070501"

def process(sc):
    sess = SparkSession(sc)

    DW = (sess.read
        .format("jdbc")
        .option("driver","org.postgresql.Driver")
        .option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/DW?sslmode=require")
        .option("dbtable", "public.aircraftutilization")
        .option("user", m_username)
        .option("password", m_password)
        .load())

    AMOS = (sess.read
        .format("jdbc")
        .option("driver","org.postgresql.Driver")
        .option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/AMOS?sslmode=require")
        .option("dbtable", "oldinstance.operationinterruption")
        .option("user", m_username)
        .option("password", m_password)
        .load())

    KPIs = (DW
        .select("aircraftid","timeid","flighthours","flightcycles","delayedminutes")
        .rdd
        .map(lambda t: ((t[1],t[0]),(float(t[2]),int(t[3]),int(t[4]))))
        .sortByKey())

    OI  = (AMOS
        .select("aircraftregistration","starttime","kind")
        .rdd
        .map(lambda t: ((t[1].date(),t[0]),t[2]))
        .filter(lambda t: t[1] in ["Delay","AircraftOnGround","Safety"])
        .sortByKey())

    input = (sc.wholeTextFiles("./resources/trainingData/*.csv")
        .map(lambda t: ((date(2000+int(t[0].split("/")[-1][4:6]),int(t[0].split("/")[-1][2:4]),int(t[0].split("/")[-1][0:2])),t[0].split("/")[-1][20:26]),list(t[1].split("\n"))))
        .flatMap(lambda t: [(t[0], value) for value in t[1]])
        .filter(lambda t: "value" not in t[1])
        .filter(lambda t: t[1] != '')
        .mapValues(lambda t: (float(t.split(";")[-1]),1))
        .reduceByKey(lambda t1,t2: (t1[0]+t2[0],t1[1]+t2[1]))
        .mapValues(lambda t: t[0]/t[1])
        .sortByKey())

    data = (OI
        .union(OI.map(lambda t: ((t[0][0] - timedelta(days=1),t[0][1]),t[1])))
        .union(OI.map(lambda t: ((t[0][0] - timedelta(days=2),t[0][1]),t[1])))
        .union(OI.map(lambda t: ((t[0][0] - timedelta(days=3),t[0][1]),t[1])))
        .union(OI.map(lambda t: ((t[0][0] - timedelta(days=4),t[0][1]),t[1])))
        .union(OI.map(lambda t: ((t[0][0] - timedelta(days=5),t[0][1]),t[1])))
        .union(OI.map(lambda t: ((t[0][0] - timedelta(days=6),t[0][1]),t[1])))
        .sortByKey()) # hacer un generador que devuelva 7

    out = (input
        .join(KPIs)
        .leftOuterJoin(data)
        .mapValues(lambda t: (t[1], t[1][1] is not None)))

    for x in out.collect():
        print(x)

	d = rdd.map(lambda t: LabeledPoint(t[1][0],t[1][0][0],t[1][0][1],t[1][0][2],t[1][0][3]]))
	#MLUtils.saveAsLibSVMFile(d, "./resources/fake/")

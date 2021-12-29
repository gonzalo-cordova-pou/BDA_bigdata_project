#------------------ IMPORTS ------------------#
import os
import sys
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import utils
import management
import analysis
import runtime
import random
import operator
from datetime import date,timedelta
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from tempfile import NamedTemporaryFile
from fileinput import input
from glob import glob
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
import re
from tempfile import NamedTemporaryFile
#---------------------------------------------#

#----- USER CREDENTIALS FOR FIB POSTGRESQL ---#
g_username = "gonzalo.cordova"
g_password = "DB060601"
m_username = "miquel.palet.lopez"
m_password = "DB070501"
#---------------------------------------------#


#---------------- FUNCTIONS ------------------#

def get_files(aircraft, date_):
    """
    Returns a list with the file names of files matching the aircraft
    and date passed as parameters
    """
    
    target_files = []
    expression = date_ + "-...-...-....-" + aircraft + ".csv"

    for root, dirs, files in os.walk("./resources/trainingData/"):
        for file in files:
            res = re.match(expression, file)
            if res:
                target_files.append(file)
    
    return target_files


def read_aircraftutilization(session):
    """
    Returns aircraft utilization table from the Data Warehouse.
    """

    return (session.read
        .format("jdbc")
        .option("driver","org.postgresql.Driver")
        .option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/DW?sslmode=require")
        .option("dbtable", "public.aircraftutilization")
        .option("user", m_username)
        .option("password", m_password)
        .load())

def read_kpis(session):
    """
    Returns FH, FC and DM KPIs for aircraft and day as RDD.
    """

    return (read_aircraftutilization(session)
        .select("aircraftid","timeid","flighthours","flightcycles","delayedminutes")
        .rdd
        .map(lambda t: ((t[1],t[0]),(float(t[2]),int(t[3]),int(t[4]))))
        .sortByKey())

def read_operationinterruption(session):
    """
    Returns operation inerruption table from the AMOS database.
    """

    return (session.read
        .format("jdbc")
        .option("driver","org.postgresql.Driver")
        .option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/AMOS?sslmode=require")
        .option("dbtable", "oldinstance.operationinterruption")
        .option("user", m_username)
        .option("password", m_password)
        .load())

def read_events(session):
    """
    Returns unscheduled maintenance events for aircraft and day logged by
    the aircraft navigation subsystem as RDD.
    """

    return  (read_operationinterruption(session)
    .select("aircraftregistration","starttime","kind","subsystem")
    .rdd
    .map(lambda t: ((t[1].date(),t[0]),(t[2],t[3])))
    .filter(lambda t: t[1][0] in ["Delay","AircraftOnGround","Safety"])
	.filter(lambda t: t[1][1] == '3453')
	.mapValues(lambda t: t[0])
    .sortByKey())

def extract_csv(sc, path):
    """
    Returns average sensor measurements for aircraft and day as RDD.
    """

    return (sc.wholeTextFiles(path)
        .map(lambda t: ((date(2000+int(t[0].split("/")[-1][4:6]), # year
                              int(t[0].split("/")[-1][2:4]),      # month
                              int(t[0].split("/")[-1][0:2])),     # day
                         t[0].split("/")[-1][20:26]),             # aircraft id
                        list(t[1].split("\n"))))
        # Separate rows for each measurement
        .flatMap(lambda t: [(t[0], value) for value in t[1]])
        .filter(lambda t: "value" not in t[1])                     # | data
        .filter(lambda t: t[1] != '')                              # | cleaning
        .mapValues(lambda t: (float(t.split(";")[-1]),1))          # |
        # Compute average measurement
        .reduceByKey(lambda t1,t2: (t1[0]+t2[0],t1[1]+t2[1]))
        .mapValues(lambda t: t[0]/t[1])
        .sortByKey())

def add_rows(df):
    """
    Generates and returns seven extra rows substracting
    1,2,...7 days from every given row.
    """

    return (df
        .flatMap(lambda t: [((t[0][0] - timedelta(days=days),t[0][1]),t[1])
                            for days in range(8)]))

#---------------------------------------------#
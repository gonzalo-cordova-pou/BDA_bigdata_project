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

#---------------------------------------------#
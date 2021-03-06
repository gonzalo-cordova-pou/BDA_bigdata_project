from utils import *


#------------------ STARTING SPARK ------------------#

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

    argument = input("\nWhat pipeline do you want to execute? (management, analysis or runtime)\n")
    

    if(argument == "management"):
        print("\n#########  STARTING MANAGEMENT PIPELINE ##########")
        management.process(sc)
        print("\nSUCCESS! Clean data created in 'LibSVM-files' directory")
    elif(argument == "analysis"):
        print("\n#########  STARTING ANALYSIS PIPELINE ##########")
        analysis.process(sc)
    elif(argument == "runtime"):
        print("\n######### STARTING RUNTIME PIPELINE ##########")
        aircraft = input("Enter an aircraft:")
        date_ = input("Enter a date (format: ddmmyy):")
        runtime.process(sc, aircraft, date_)
    else:
       print("Wrong pipeline. Options: management, analysis, runtime")
       exit()

#---------------------------------------------------------------------------------#
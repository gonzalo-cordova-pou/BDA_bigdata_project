import pyspark
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

g_username = "gonzalo.cordova"
g_password = "DB060601"
m_username = "miquel.palet.lopez"
m_password = "DB070501"

def process(sc):
	sess = SparkSession(sc)

	AIMS = (sess.read
		.format("jdbc")
		.option("driver","org.postgresql.Driver")
		.option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/AIMS?sslmode=require")
		.option("dbtable", "public.flights")
		.option("user", m_username)
		.option("password", m_password)
		.load())

	input = (sc.textFile("./resources/trainingData/*.csv")
		.filter(lambda t: "date" not in t)
		.map(lambda t: (input_file_name()[0:6],float(t.split(";")[3])))
		.cache())

	for x in input.collect():
		print(x)

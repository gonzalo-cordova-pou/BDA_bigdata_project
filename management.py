import pyspark
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession

AIMSusername = "BDAgonzalo.cordova"
AIMSpassword = "DB060601"

def process(sc):
	sess = SparkSession(sc)

	AIMS = (sess.read
		.format("jdbc")
		.option("driver","org.postgresql.Driver")
		.option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/AIMS?sslmode=require")
		.option("dbtable", "public.flights")
		.option("user", AIMSusername)
		.option("password", AIMSpassword)
		.load())

	count = (AIMS.
		select("departureairport")
		.rdd
		.map(lambda t: t[0])
		.distinct()
		.count())
	print(str(count) + " airports with at least one departure")

import pyspark
import operator
from pyspark.sql import SparkSession

g_username = "gonzalo.cordova"
g_password = "DB060601"
m_username = "miquel.palet.lopez"
m_password = "DB070501"

def process(sc):
	sess = SparkSession(sc)

	AMOS = (sess.read
		.format("jdbc")
		.option("driver","org.postgresql.Driver")
		.option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/AMOS?sslmode=require")
		.option("dbtable", "public.flights")
		.option("user", g_username)
		.option("password", g_password)
		.load())

	input = (sc.wholeTextFiles("./resources/trainingData/*.csv")
		.filter(lambda t: "date" not in t)
		.map(lambda t: ((t[0].split("/")[-1][0:6],t[0].split("/")[-1][7:13]),t[1].split(";")[4].split("\n")[0])) #regex
		.cache())

	for x in input.collect():
		print(x)
	
	count = (AMOS.
		select("airport")
		.rdd
		.map(lambda t: t[0])
		.distinct()
		.count())

	print(str(count) + " airports with at least one departure")

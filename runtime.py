"""
RUNTIME PIPELINE

Steps:
        1. Replicate data management pipeline
        2. Classify the record and output maintenance / no maintenance

"""


def process(sc, aircraft, date_):

    # ------------- 1. REPLICATE DATA MANAGEMENT PIPELINE -------------


    # Create a Spark Session with the context given as parameter
    sess = SparkSession(sc)

    # Load the Decision Tree Model (created in the analysis pipeline)
    model = DecisionTreeModel.load(sc, "myDecisionTreeClassificationModel")

    # Load the DW
    DW = (sess.read
		.format("jdbc")
		.option("driver","org.postgresql.Driver")
		.option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/DW?sslmode=require")
		.option("dbtable", "public.aircraftutilization")
		.option("user", g_username)
		.option("password", g_password)
		.load())
    
    # Extract and process KPIs
    KPIs = (DW
        .select("aircraftid","timeid","flighthours","flightcycles","delayedminutes")
        .rdd
        .map(lambda t: ((t[1],t[0]),(float(t[2]),int(t[3]),int(t[4]))))
        .sortByKey())
    
    # Get the csv files (sensor data) matching the aircraft and date
    target_files = get_files(aircraft, date_)
    
    # Get the sensor data for the aircraft
    CSVfile = (sc.wholeTextFiles("./resources/trainingData/" + target_files[0])
        .map(lambda t: ((date(2000+int(t[0].split("/")[-1][4:6]),int(t[0].split("/")[-1][2:4]),int(t[0].split("/")[-1][0:2])),t[0].split("/")[-1][20:26]),list(t[1].split("\n"))))
        .flatMap(lambda t: [(t[0], value) for value in t[1]])
        .filter(lambda t: "value" not in t[1])
        .filter(lambda t: t[1] != '')
        .mapValues(lambda t: (float(t.split(";")[-1]),1))
        .reduceByKey(lambda t1,t2: (t1[0]+t2[0],t1[1]+t2[1]))
        .mapValues(lambda t: t[0]/t[1])
        .sortByKey())

    # Join sensor data and KPIs
    rdd = (CSVfile
		.join(KPIs))
    

    # ------------- 2. CLASSIFY THE RECORD  -------------
    

    # Format the features [sensor_mean, flighthours, flightcycles, delayedminutes]
    t =  rdd.collect()[0]
    sample = [t[1][0],t[1][1][0],t[1][1][1],t[1][1][2]]
    
    # Predict the maintenance
    prediction = model.predict(sample)

    # Print results
    print("\n \n")
    print("Sample:  ", sample)
    if (prediction == 0):
        print("predicted result:   NO MAINTENANCE")
        print("\n \n")
        print("**Miquel Palet and Gonzalo Cordova are not responsible for the problems (or even deaths) that the bad perforance of this model may cause**")
    else:
        print("predicted result: MAINTENANCE")
        print("\n \n")
        print("**Miquel Palet and Gonzalo Cordova are not responsible for the problems (or even deaths) that the bad perforance of this model may cause**")
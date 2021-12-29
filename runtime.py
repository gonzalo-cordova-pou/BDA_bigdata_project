from utils import *

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
    model = utils.DecisionTreeModel.load(sc, "myDecisionTreeClassificationModel")
    
    # Extract and process KPIs
    KPIs = utils.read_kpis(sess)
    
    # Get the csv files (sensor data) matching the aircraft and date
    target_files = utils.get_files(aircraft, date_)
    
    # Get the sensor data for the aircraft
    CSVfile = utils.extract_csv(sc, "./resources/trainingData/" + target_files[0])

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
    else:
        print("predicted result: MAINTENANCE")
    print("\n \n")
    print("**Miquel Palet and Gonzalo Cordova are not responsible for the problems (or even deaths) that the bad perforance of this model may cause**")
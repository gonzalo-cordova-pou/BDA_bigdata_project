from utils import *

"""
MANAGEMENT PIPELINE

Steps:
        1. Read sensor measurements from CSV files related to a certain
           aircraft and average it per day.
        2. Enrich with the KPIs from the Data Warehouse.
        3. Label each row with either unscheduled maintenance or no
           maintenance predicted in the next 7 days for that flight.
        4. Generate a matrix with the gathered data and store it.

"""

def process(sc):

    sess = SparkSession(sc)

    KPIs = utils.read_kpis(sess, "x")

    events  = utils.add_rows(utils.read_events(sess))

    # ------------- 1. READ CSV FILES -------------

    CSVfiles = utils.extract_csv(sc, "./resources/trainingData/*.csv")


    output = (CSVfiles

    # ------------- 2. ENRICH WITH KPIs -------------

        .join(KPIs)

    # ------------- 3. LABEL ROWS -------------

        .leftOuterJoin(events)
        # Format labels as binary (0 for no maintenance, 1 for maintenance)
        .mapValues(lambda t: (t[0], t[1] is not None))

    # ------------- 4. FORMAT AND STORE DATA -------------

        # Format data with LibSVM format (label index1:value1 index2:value2 ...)
        .map(lambda t: utils.LabeledPoint(t[1][1],[t[1][0][0],t[1][0][1][0],
                                             t[1][0][1][1],t[1][0][1][2]])))

    # Delete 'LibSVM-files' folder if exists
    if os.path.exists('./LibSVM-files/'):
        utils.shutil.rmtree('./LibSVM-files/')
    
    utils.MLUtils.saveAsLibSVMFile(output, "./LibSVM-files/")
    print()

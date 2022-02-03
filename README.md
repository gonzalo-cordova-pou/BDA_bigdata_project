# Project: Big Data (Predictive Analysis)

This is a university project for Advanced Databases. We use PySpark (RDD structure) to create different pipelines that read from PostgreSQL DB and CSV files to create a Decision Tree Classifier.

## Authors
* **Miquel Palet López**
* **Gonzalo Córdova Pou**

## Files
- _main.py_: Main file from which you can acces to the pipelines. No parameter is needed to execute it.
- _utils.py_: Imported in every other file. It contains the auxiliary functions we have created and all 'import's necessary.
- _mangement.py_: Management pipeline. Execute _main.py_ and later select option 'management'.
- _analysis.py_: Analysis pipeline. Execute _main.py_ and later select option 'management'.
- _runtime.py_: Runtime pipeline. Execute _main.py_ and later select option 'management'.

## Sketches
  - **See the sketches in the Assumptions.pdf file**

## Assumptions
* **General Pipeline Assumptions:**
    - User is connected (or knows how) to the FIB PostgreSQL.
    - Sensor data is in csv file with name in format date-airport-airport-4digits-aircraft.csv
    <br> example: 010615-FUE-TXL-3573-XY-YCV
* **Management Pipeline Assumptions:**
    - All sensor data is located in the './resources/trainingData/' path.
* **Analysis Pipeline Assumptions:**
    - You have succesfully executed Management Pipeline.
    - There is one and only one csv file for each aircraft-date pair.
    - (impurity='gini', maxDepth=5, maxBins=32) are good hyperparameters for the Decision Tree.
* **Runtime Pipeline Assumptions:**
    - It is assumed that you have succesfully executed Analysis Pipeline.

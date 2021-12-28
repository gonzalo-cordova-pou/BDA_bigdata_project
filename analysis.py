"""
Steps:
        1. Load and parse data
        2. Split into training and test
        3. Train the model
        4. Evaluate on test
        5. Print results
        6. Save the model

Notes: 
    For the execution of this pipeline you will have to have executed the management pipeline first.
    Management pipeline will create a directory with libsvm files which will be loaded in this pipeline.
"""
import pyspark
import operator
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark.sql import SparkSession

def process(sc):
    sess = SparkSession(sc)

    # 1. Load and parse the data file into an RDD of LabeledPoint.
    data = MLUtils.loadLibSVMFile(sc, './LibSVM-files/')

    # 2. Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # 3. Train a DecisionTree model
    model = DecisionTree.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={}, impurity='gini', maxDepth=5, maxBins=32)

    # 4. Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(
        lambda lp: lp[0] != lp[1]).count() / float(testData.count())

    # 5. Print results
    print("\n")
    print('Test Error = ' + str(testErr))
    print("\n")
    print('Learned classification tree model:')
    print(model.toDebugString())

    # 6. Save and load model
    model.save(sc, "myDecisionTreeClassificationModel")
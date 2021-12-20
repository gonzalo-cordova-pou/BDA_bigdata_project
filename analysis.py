from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
import pyspark
import random
import operator
from pyspark.sql import SparkSession
from datetime import datetime,timedelta
from pyspark.mllib.regression import LabeledPoint

def process(sc):
    sess = SparkSession(sc)
    # Load and parse the data file into an RDD of LabeledPoint.
    data = MLUtils.loadLibSVMFile(sc, './gatita/')
    (trainingData, testData) = data.randomSplit([0.7, 0.3])
    # Split the data into training and test sets (30% held out for testing)

    # Train a DecisionTree model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    model = DecisionTree.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={}, impurity='gini', maxDepth=5, maxBins=32)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(
        lambda lp: lp[0] != lp[1]).count() / float(testData.count())

    print('Test Error = ' + str(testErr))
    print('Learned classification tree model:')
    print(model.toDebugString())

    # Save and load model
    model.save(sc, "myDecisionTreeClassificationModel")
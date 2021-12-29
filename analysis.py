"""
ANALYSIS PIPELINE

Steps:
        1. Create and split datasets
        2. Train and test the model
        4. Store validated model 

Notes: 
    For the execution of this pipeline you will have to have executed the management pipeline first.
    Management pipeline will create a directory with libsvm files which will be loaded in this pipeline.
"""


def process(sc):
    sess = SparkSession(sc)

    # ------------- 1. CREATE AND SPLIT DATASETS -------------

    # Load and parse the data file into an RDD of LabeledPoint.
    data = MLUtils.loadLibSVMFile(sc, './LibSVM-files/')

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    
    # ------------- 2. TRAIN AND TEST MODEL -------------

    # Train a DecisionTree model
    model = DecisionTree.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={}, impurity='gini', maxDepth=5, maxBins=32)

    # Evaluate model on test instances
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)

    # Error count
    Err = labelsAndPredictions.filter(
        lambda lp: lp[0] != lp[1])
    
    # False negatives count
    false_negatives = Err.filter(
        lambda lp: lp[1] == 0).count()
    
    # True positive count
    true_positives = labelsAndPredictions.filter(
        lambda lp: (lp[0] == 1) & (lp[0] == lp[1])).count()
    
    # Compute Test Accuracy and Recall
    recall = true_positives / float(true_positives + false_negatives)
    testAcc = 1 - (Err.count() / float(testData.count()))

    # Print results
    print("\n")
    print('Test Accuracy = ' + str(round(testAcc, 2)))
    print('Recall = ' + str(round(recall,2)))
    print("\n")
    print('Learned classification tree model:')
    print(model.toDebugString())



    # ------------- 3. STORE VALIDATED MODEL -------------

    # Save model
    model.save(sc, "myDecisionTreeClassificationModel")
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils


sameModel = DecisionTreeModel.load(sc, "myDecisionTreeClassificationModel")


# PARAMETERS FOR model.predict
#  ---------------------------------------
#    X  ->  {array-like, sparse matrix} of shape (n_samples, n_features)
#              The input samples. Internally, it will be converted to dtype=np.float32 and if a sparse matrix is provided to a sparse csr_matrix.
# check_input ->  bool, default=True
#        Allow to bypass several input checking. Don’t use this parameter unless you know what you do.

# Returns
#    y  ->  array-like of shape (n_samples,) or (n_samples, n_outputs)
#              The predicted classes, or the predict values.

# Evaluate model on test instances and compute test error


predictions = model.predict(inputData.map(lambda x: x.features))
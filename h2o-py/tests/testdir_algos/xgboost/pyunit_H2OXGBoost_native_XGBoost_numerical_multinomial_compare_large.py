import pandas as pd
import xgboost as xgb
import time
import random

from h2o.estimators.xgboost import *
from tests import pyunit_utils

'''
The goal of this test is to compare the results of H2OXGBoost and natibve XGBoost for multinomial classification. 
The dataset contains only numerical columns.
'''
def comparison_test():
    assert H2OXGBoostEstimator.available() is True
    runSeed = random.randint(1, 1073741824)
    ntrees = 10
    nrows = 100000
    ncols = 10
    responseF = 5

    h2oParamsD = {"ntrees":ntrees, "max_depth":4, "seed":runSeed, "learn_rate":0.7, "col_sample_rate_per_tree" : 0.9,
                 "min_rows" : 5, "score_tree_interval": ntrees+1, "dmatrix_type":"dense"}
    nativeParam = {'colsample_bytree': h2oParamsD["col_sample_rate_per_tree"],
                   'tree_method': 'auto',
                   'seed': h2oParamsD["seed"],
                   'booster': 'gbtree',
                   'objective': 'multi:softprob',
                   'lambda': 0.0,
                   'eta': h2oParamsD["learn_rate"],
                   'grow_policy': 'depthwise',
                   'alpha': 0.0,
                   'subsample': 1.0,
                   'colsample_bylevel': 1.0,
                   'max_delta_step': 0.0,
                   'min_child_weight': h2oParamsD["min_rows"],
                   'gamma': 0.0,
                   'max_depth': h2oParamsD["max_depth"],
                   'num_class':responseF}

    trainFile = pyunit_utils.genTrainFiles(nrows, ncols, responseLevel=responseF)     # load in dataset and add response column
    myX = trainFile.names
    y='response'
    myX.remove(y)

    h2oModelD = H2OXGBoostEstimator(**h2oParamsD)
    # gather, print and save performance numbers for h2o model
    h2oModelD.train(x=myX, y=y, training_frame=trainFile)
    h2oTrainTimeD = h2oModelD._model_json["output"]["run_time"]
    time1 = time.time()
    h2oPredictD = h2oModelD.predict(trainFile)
    h2oPredictTimeD = time.time()-time1

    # train the native XGBoost
    nativeTrain = pyunit_utils.genDMatrix_all_numerics(trainFile, myX, y)
    nativeModel = xgb.train(params=nativeParam,
                            dtrain=nativeTrain)
    nativeTrainTime = time.time()-time1
    time1=time.time()
    nativePred = nativeModel.predict(data=nativeTrain, ntree_limit=ntrees)
    nativeScoreTime = time.time()-time1

    pyunit_utils.summarizeResult_multinomial(h2oPredictD, nativePred, h2oTrainTimeD, nativeTrainTime,
                                             h2oPredictTimeD, nativeScoreTime, tolerance=1e-6)

if __name__ == "__main__":
    pyunit_utils.standalone_test(comparison_test)
else:
    comparison_test()
#!/usr/bin/python
# RuntimeError: Broken toolchain: cannot link a simple C program
# apk update
# apk add make automake gcc g++ subversion python3-dev
# pip3 install numpy
# spark/bin/spark-submit spark/primeri/validation.py

import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import isnull, when, count, col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.pipeline import PipelineModel


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

warehouse_location = os.path.abspath('/user/hive/warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark Training") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

quiet_logs(spark)

spark.sql("use nvo")

train_data = [[-2.590788483636364, 1.8141996581818198, 9.174627490909101, 7.409731462120533e-05, 0.4093115420521371, 4.8146917368492717e-05, 0.260238598843446, -2.704562871482821e-05, 0.24746339126340092, 0.002480183246607139, -0.0057317339504464266, 0.000965834772321428, 1.8190205766751955e-05, 0.017545573310573152, -7.254274370518142e-05, 0.0417352859955709, -4.961063906834331e-05, 0.040953201648551325, 'stand'], [-2.352029436702126, 1.7663233414893633, 9.333818228723414, 0.0003605875276837228, 0.3039724301543759, -0.0001830905784114591, 0.23343795144234597, 0.0002691327691996174, 0.28384260290751817, 0.008919830377808228, -0.00734506203, -0.004442786761552512, -1.2207451312697002e-05, 0.06836377059224552, 2.125488848661324e-05, 0.04074375902936589, -8.307297036201608e-06, 0.025993300703875514, 'stand'], [-2.229537098901105, 1.731116733333334, 9.419416923076918, -4.109683459703106e-05, 0.283713357614371, -2.854729789512212e-05, 0.23169431054016698, -3.3641490909513925e-05, 0.29050930359031724, 0.000559365829735973, 0.0024843219764686454, -0.005102736502013199, -1.5828695669987756e-06, 0.005524346697826814, 5.30757350321539e-06, 0.016727938167941093, 8.281751205607519e-06, 0.015290396606594907, 'stand'], [-2.1737907595652177, 1.654921877391305, 9.478757476086951, -5.449361383729736e-06, 0.2209617887120057, -8.457624791461185e-05, 0.20143757540197904, 3.395091803795799e-05, 0.24272382653638416, 0.0005290203594715444, 0.00603911386723577, -0.0053262077168292656, 1.2162057744327227e-07, 0.0036965849651164817, -6.031826851179319e-06, 0.010371105736369065, -8.18006954810107e-06, 0.017067612835862656, 'stand']]
train_data_columns = [
    "mean_x_acc", "mean_y_acc", "mean_z_acc", "coef_x_acc", "rmse_x_acc", "coef_y_acc", "rmse_y_acc", "coef_z_acc", "rmse_z_acc", 
    "mean_x_gyro", "mean_y_gyro", "mean_z_gyro", "coef_x_gyro", "rmse_x_gyro", "coef_y_gyro", "rmse_y_gyro", "coef_z_gyro", "rmse_z_gyro", 
    "target_str"
]

train_df = spark.createDataFrame(train_data, train_data_columns)

train_df.show()

train_df = StringIndexer(
    inputCol='target_str', 
    outputCol='target', 
    handleInvalid='keep').fit(train_df).transform(train_df).drop("target_str")

transformed_data = VectorAssembler(inputCols=train_data_columns[:-1], outputCol='features').transform(train_df)

mPath =  "/home/modelsave"

persistedModel = RandomForestClassificationModel.load(mPath)

# predict
predictions = persistedModel.transform(transformed_data)

evaluator = MulticlassClassificationEvaluator(
    labelCol='target', 
    predictionCol='prediction', 
    metricName='accuracy')

accuracy = evaluator.evaluate(predictions)
print('Test Accuracy = ', accuracy)

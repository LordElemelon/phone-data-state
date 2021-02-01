#!/usr/bin/python
# RuntimeError: Broken toolchain: cannot link a simple C program
# apk update
# apk add make automake gcc g++ subversion python3-dev
# 
# apt update
# apt install build-essential
# apt-get install python3.4-dev
# 
# pip3 install numpy
# spark/bin/spark-submit spark/primeri/training.py
# spark/bin/spark-submit --packages org.apache.spark:spark-hive_2.11:2.3.2 org.apache.avro:avro-mapred:1.7.7 spark/primeri/training.py

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
    .config("spark.sql.hive.metastore.version", "2.3.2") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

quiet_logs(spark)

spark.sql("use nvo")
train_acc = spark.sql("select * from train_acc_parq")
train_gyro = spark.sql("select * from train_gyro_parq")

persons = [x.person for x in train_acc.select("person").distinct().collect()]
print(persons)

train_acc.show(5)
print(train_acc.count())
print(train_acc.columns)
print(train_acc.dtypes)

minmaxvals = {}
for row in train_acc.select(["time", "person"]).groupBy(["person"]).min().join(train_acc.select(["time", "person"]).groupBy(["person"]).max(), "person").collect():
    minmaxvals[row.person] = {"min_time": row["min(time)"], "max_time": row["max(time)"]}
print(minmaxvals)
totalmin = min([x["min_time"] for x in minmaxvals.values()])

train_acc = train_acc.select(train_acc.time, (train_acc.time - totalmin).alias("sttime"), train_acc.x, train_acc.y, train_acc.z, train_acc.person, train_acc.target) \
    .select(train_acc.time, col("sttime").cast("int"), train_acc.x, train_acc.y, train_acc.z, train_acc.person, train_acc.target)
    
train_gyro = train_gyro.select(train_gyro.time, (train_gyro.time - totalmin).alias("sttime"), train_gyro.x, train_gyro.y, train_gyro.z, train_gyro.person, train_gyro.target) \
    .select(train_gyro.time, col("sttime").cast("int"), train_gyro.x, train_gyro.y, train_gyro.z, train_gyro.person, train_gyro.target)

train_acc.show()
train_gyro.show()

train_data = []

for person in persons:
    lower = minmaxvals[person]["min_time"]
    higher = lower + 5000
    print(person)
    i = 0
    while higher < minmaxvals[person]["max_time"]:
        # acc
        rows = train_acc.filter(train_acc.person == person).filter(train_acc.time >= lower).filter(train_acc.time <= higher)
        if rows.count() < 20:
            lower = higher + 15000
            higher = min(lower+5000, minmaxvals[person]["max_time"])
            continue
        
        training = VectorAssembler(inputCols=["sttime"], outputCol="features").transform(rows)

        lr_x = LinearRegression(labelCol="x", featuresCol="features", \
            maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8)
        lrModel = lr_x.fit(training)
        x_coef_acc = lrModel.coefficients[0]
        x_rmse_acc = lrModel.summary.rootMeanSquaredError

        lr_y = LinearRegression(labelCol="y", featuresCol="features", \
            maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8)
        lrModel = lr_y.fit(training)
        y_coef_acc = lrModel.coefficients[0]
        y_rmse_acc = lrModel.summary.rootMeanSquaredError

        lr_z = LinearRegression(labelCol="z", featuresCol="features", \
            maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8)
        lrModel = lr_z.fit(training)
        z_coef_acc = lrModel.coefficients[0]
        z_rmse_acc = lrModel.summary.rootMeanSquaredError

        chosen_target = rows.groupBy("target").count().orderBy(["count"], ascending=False).first().target

        means_acc = rows.groupBy().mean("x", "y", "z").first()

        # gyro
        rows = train_gyro.filter(train_gyro.person == person).filter(train_gyro.time >= lower).filter(train_gyro.time <= higher)

        training = VectorAssembler(inputCols=["sttime"], outputCol="features").transform(rows)

        lr_x = LinearRegression(labelCol="x", featuresCol="features", \
            maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8)
        lrModel = lr_x.fit(training)
        x_coef_gyro = lrModel.coefficients[0]
        x_rmse_gyro = lrModel.summary.rootMeanSquaredError

        lr_y = LinearRegression(labelCol="y", featuresCol="features", \
            maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8)
        lrModel = lr_y.fit(training)
        y_coef_gyro = lrModel.coefficients[0]
        y_rmse_gyro = lrModel.summary.rootMeanSquaredError

        lr_z = LinearRegression(labelCol="z", featuresCol="features", \
            maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8)
        lrModel = lr_z.fit(training)
        z_coef_gyro = lrModel.coefficients[0]
        z_rmse_gyro = lrModel.summary.rootMeanSquaredError

        means_gyro = rows.groupBy().mean("x", "y", "z").first()

        train_data.append((
            float(means_acc["avg(x)"]), float(means_acc["avg(y)"]), float(means_acc["avg(z)"]),
            float(x_coef_acc), float(x_rmse_acc), float(y_coef_acc), float(y_rmse_acc), float(z_coef_acc), float(z_rmse_acc),
            float(means_gyro["avg(x)"]), float(means_gyro["avg(y)"]), float(means_gyro["avg(z)"]),
            float(x_coef_gyro), float(x_rmse_gyro), float(y_coef_gyro), float(y_rmse_gyro), float(z_coef_gyro), float(z_rmse_gyro),
            chosen_target
        ))
        if i % 5 == 0:
            print("{:.2f}".format(100*(lower-minmaxvals[person]["min_time"])/(minmaxvals[person]["max_time"]-minmaxvals[person]["min_time"])))
        i = i + 1

        lower = higher + 15000
        higher = min(lower+5000, minmaxvals[person]["max_time"])

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

training_data = VectorAssembler(inputCols=train_data_columns[:-1], outputCol='features').transform(train_df)

rf = RandomForestClassifier(labelCol='target', 
                            featuresCol='features',
                            maxDepth=5)

model = rf.fit(training_data)
mPath =  "/home/modelspersist"
model.write().overwrite().save(mPath)

persistedModel = RandomForestClassificationModel.load(mPath)

# predict
predictions = persistedModel.transform(training_data)

evaluator = MulticlassClassificationEvaluator(
    labelCol='target', 
    predictionCol='prediction', 
    metricName='accuracy')

accuracy = evaluator.evaluate(predictions)
print('Test Accuracy = ', accuracy)


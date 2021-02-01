r"""
 Run the example
    `spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 spark/primeri/streaming.py zoo1:2181`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
import json
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: kafka_wordcount.py <zk>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="SparkStreamingAlg")
    quiet_logs(sc)
    ssc = StreamingContext(sc, 15)

    ssc.checkpoint("stateful_checkpoint_direcory")

    zooKeeper = sys.argv[1]
    kvs = KafkaUtils.createStream(ssc, zooKeeper, "spark-streaming-consumer", {"phone_data": 1})

    def process(time, rdd):
        print("========= %s =========" % str(time))
        str_data = [(rddi[0], json.loads(rddi[1])) for rddi in rdd.collect()]
        if len(str_data) < 20:
            return
        min_time = min([rddi[1][0] for rddi in str_data])
        acc_data = [Row(timestamp=poin[1][0]-min_time, x=poin[1][1], y=poin[1][2], z=poin[1][3], person=poin[1][4], target=poin[1][5]) for poin in str_data if poin[0] == "acc"]
        gyro_data = [Row(timestamp=poin[1][0]-min_time, x=poin[1][1], y=poin[1][2], z=poin[1][3], person=poin[1][4], target=poin[1][5]) for poin in str_data if poin[0] == "gyro"]
        
        # print(acc_data[:3])
        # print(gyro_data[:3])

        schema = StructType([StructField("timestamp", LongType(), True),
            StructField("x", FloatType(), True),
            StructField("y", FloatType(), True),
            StructField("z", FloatType(), True),
            StructField("person", StringType(), True),
            StructField("target", StringType(), True)
            ])
        
        try:
            spark = getSparkSessionInstance(rdd.context.getConf())

            acc_df = spark.createDataFrame(acc_data[:], schema=schema)
            gyro_df = spark.createDataFrame(gyro_data[:], schema=schema)

            # print(acc_df.count())
            # acc_df.show()
            # gyro_df.show()

            training = VectorAssembler(inputCols=["timestamp"], outputCol="features").transform(acc_df)

            lr_x = LinearRegression(labelCol="x", featuresCol="features", \
                maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8, solver="normal")
            lrModel = lr_x.fit(training)
            x_coef_acc = lrModel.coefficients[0]
            x_rmse_acc = lrModel.summary.rootMeanSquaredError
            print(x_coef_acc, x_rmse_acc)

            lr_y = LinearRegression(labelCol="y", featuresCol="features", \
                maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8, solver="normal")
            lrModel = lr_y.fit(training)
            y_coef_acc = lrModel.coefficients[0]
            y_rmse_acc = lrModel.summary.rootMeanSquaredError
            print(y_coef_acc, y_rmse_acc)

            lr_z = LinearRegression(labelCol="z", featuresCol="features", \
                maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8, solver="normal")
            lrModel = lr_z.fit(training)
            z_coef_acc = lrModel.coefficients[0]
            z_rmse_acc = lrModel.summary.rootMeanSquaredError
            print(z_coef_acc, z_rmse_acc)

            chosen_target = acc_df.groupBy("target").count().orderBy(["count"], ascending=False).first().target

            means_acc = acc_df.groupBy().mean("x", "y", "z").first()

            # gyro

            training = VectorAssembler(inputCols=["timestamp"], outputCol="features").transform(gyro_df)

            lr_x = LinearRegression(labelCol="x", featuresCol="features", \
                maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8, solver="normal")
            lrModel = lr_x.fit(training)
            x_coef_gyro = lrModel.coefficients[0]
            x_rmse_gyro = lrModel.summary.rootMeanSquaredError
            print(x_coef_gyro, x_rmse_gyro)

            lr_y = LinearRegression(labelCol="y", featuresCol="features", \
                maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8, solver="normal")
            lrModel = lr_y.fit(training)
            y_coef_gyro = lrModel.coefficients[0]
            y_rmse_gyro = lrModel.summary.rootMeanSquaredError
            print(y_coef_gyro, y_rmse_gyro)

            lr_z = LinearRegression(labelCol="z", featuresCol="features", \
                maxIter=100, regParam=0.0, elasticNetParam=0.0, standardization=True, tol=1e-8, solver="normal")
            lrModel = lr_z.fit(training)
            z_coef_gyro = lrModel.coefficients[0]
            z_rmse_gyro = lrModel.summary.rootMeanSquaredError
            print(z_coef_gyro, z_rmse_gyro)

            means_gyro = gyro_df.groupBy().mean("x", "y", "z").first()

            datapiece = (float(means_acc["avg(x)"]), float(means_acc["avg(y)"]), float(means_acc["avg(z)"]),
                float(x_coef_acc), float(x_rmse_acc), float(y_coef_acc), float(y_rmse_acc), float(z_coef_acc), float(z_rmse_acc),
                float(means_gyro["avg(x)"]), float(means_gyro["avg(y)"]), float(means_gyro["avg(z)"]),
                float(x_coef_gyro), float(x_rmse_gyro), float(y_coef_gyro), float(y_rmse_gyro), float(z_coef_gyro), float(z_rmse_gyro),
                chosen_target
            )
            print(datapiece)

        except Exception as e:
            print("Jesam ovde?")
            print(e)
            pass
    
    kvs.foreachRDD(process)

    # acc_rdds = kvs.map(lambda x: x[1] if x[0] == "acc")
    # hyro_rdds = kvs.map(lambda x: x[1] if x[0] == "gyro")
    
    # words = lines.flatMap(lambda line: line.split(" ")) \
    #     .filter(lambda word: word.strip())\
    #     .filter(lambda word: not(word.lower() in stopWordList))\
    #     .map(lambda word: (word, 1))\
    #     .updateStateByKey(updateFunc)
        
    #  # Convert RDDs of the words DStream to DataFrame and run SQL query
    # def process(time, rdd):
    #     print("========= %s =========" % str(time))


    #     schemaString = "word value"
    #     fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    #     schema = StructType(fields)

    #     try:
    #         # Get the singleton instance of SparkSession
    #         spark = getSparkSessionInstance(rdd.context.getConf())

    #         # Convert RDD[String] to RDD[Row] to DataFrame
    #         rowRdd = rdd.map(lambda w: Row(word=w[0], value=w[1]))

    #         wordsDataFrame = spark.createDataFrame(rowRdd, schema=schema)

    #         # Creates a temporary view using the DataFrame.
    #         wordsDataFrame.createOrReplaceTempView("words")

    #         # Do word count on table using SQL and print it
    #         wordCountsDataFrame = \
    #             spark.sql("select word, value from words where value >= 10 order by value desc limit 5")

    #         wordCountsDataFrame.show()

    #     except Exception as e:
    #         print(e)
    #         pass

    

    # words.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
--beeline -u jdbc:hive2://localhost:10000

CREATE DATABASE nvo;

USE nvo;

CREATE EXTERNAL TABLE batch_acc_ext_a (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_acc/a.csv" overwrite into table batch_acc_ext_a;

SELECT *
FROM batch_acc_ext_a
LIMIT 10;

CREATE EXTERNAL TABLE batch_acc_ext_b (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_acc/b.csv" overwrite into table batch_acc_ext_b;

SELECT *
FROM batch_acc_ext_b
LIMIT 10;

CREATE EXTERNAL TABLE batch_acc_ext_c (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_acc/c.csv" overwrite into table batch_acc_ext_c;

SELECT *
FROM batch_acc_ext_c
LIMIT 10;

CREATE EXTERNAL TABLE batch_acc_ext_d (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_acc/d.csv" overwrite into table batch_acc_ext_d;

SELECT *
FROM batch_acc_ext_d
LIMIT 10;

CREATE EXTERNAL TABLE batch_acc_ext_e (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_acc/e.csv" overwrite into table batch_acc_ext_e;

SELECT *
FROM batch_acc_ext_e
LIMIT 10;

CREATE EXTERNAL TABLE batch_acc_ext_f (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_acc/f.csv" overwrite into table batch_acc_ext_f;

SELECT *
FROM batch_acc_ext_f
LIMIT 10;

CREATE EXTERNAL TABLE batch_acc_ext_g (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_acc/g.csv" overwrite into table batch_acc_ext_g;

SELECT *
FROM batch_acc_ext_g
LIMIT 10;

CREATE EXTERNAL TABLE batch_acc_ext_h (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_acc/h.csv" overwrite into table batch_acc_ext_h;

SELECT *
FROM batch_acc_ext_h
LIMIT 10;

CREATE EXTERNAL TABLE batch_acc_ext_i (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_acc/i.csv" overwrite into table batch_acc_ext_i;

SELECT *
FROM batch_acc_ext_i
LIMIT 10;

CREATE EXTERNAL TABLE batch_gyro_ext_a (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_gyro/a.csv" overwrite into table batch_gyro_ext_a;

SELECT *
FROM batch_gyro_ext_a
LIMIT 10;

CREATE EXTERNAL TABLE batch_gyro_ext_b (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_gyro/b.csv" overwrite into table batch_gyro_ext_b;

SELECT *
FROM batch_gyro_ext_b
LIMIT 10;

CREATE EXTERNAL TABLE batch_gyro_ext_c (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_gyro/c.csv" overwrite into table batch_gyro_ext_c;

SELECT *
FROM batch_gyro_ext_c
LIMIT 10;

CREATE EXTERNAL TABLE batch_gyro_ext_d (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_gyro/d.csv" overwrite into table batch_gyro_ext_d;

SELECT *
FROM batch_gyro_ext_d
LIMIT 10;

CREATE EXTERNAL TABLE batch_gyro_ext_e (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_gyro/e.csv" overwrite into table batch_gyro_ext_e;

SELECT *
FROM batch_gyro_ext_e
LIMIT 10;

CREATE EXTERNAL TABLE batch_gyro_ext_f (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_gyro/f.csv" overwrite into table batch_gyro_ext_f;

SELECT *
FROM batch_gyro_ext_f
LIMIT 10;

CREATE EXTERNAL TABLE batch_gyro_ext_g (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_gyro/g.csv" overwrite into table batch_gyro_ext_g;

SELECT *
FROM batch_gyro_ext_g
LIMIT 10;

CREATE EXTERNAL TABLE batch_gyro_ext_h (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_gyro/h.csv" overwrite into table batch_gyro_ext_h;

SELECT *
FROM batch_gyro_ext_h
LIMIT 10;

CREATE EXTERNAL TABLE batch_gyro_ext_i (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/batch_gyro/a.csv" overwrite into table batch_gyro_ext_i;

SELECT *
FROM batch_gyro_ext_i
LIMIT 10;

CREATE EXTERNAL TABLE train_acc_ext_a (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_acc/a.csv" overwrite into table train_acc_ext_a;

SELECT *
FROM train_acc_ext_a
LIMIT 10;

CREATE EXTERNAL TABLE train_acc_ext_b (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_acc/b.csv" overwrite into table train_acc_ext_b;

SELECT *
FROM train_acc_ext_b
LIMIT 10;

CREATE EXTERNAL TABLE train_acc_ext_c (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_acc/c.csv" overwrite into table train_acc_ext_c;

SELECT *
FROM train_acc_ext_c
LIMIT 10;

CREATE EXTERNAL TABLE train_acc_ext_d (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_acc/d.csv" overwrite into table train_acc_ext_d;

SELECT *
FROM train_acc_ext_d
LIMIT 10;

CREATE EXTERNAL TABLE train_acc_ext_e (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_acc/e.csv" overwrite into table train_acc_ext_e;

SELECT *
FROM train_acc_ext_e
LIMIT 10;

CREATE EXTERNAL TABLE train_acc_ext_f (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_acc/f.csv" overwrite into table train_acc_ext_f;

SELECT *
FROM train_acc_ext_f
LIMIT 10;

CREATE EXTERNAL TABLE train_acc_ext_g (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_acc/g.csv" overwrite into table train_acc_ext_g;

SELECT *
FROM train_acc_ext_g
LIMIT 10;

CREATE EXTERNAL TABLE train_acc_ext_h (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_acc/h.csv" overwrite into table train_acc_ext_h;

SELECT *
FROM train_acc_ext_h
LIMIT 10;

CREATE EXTERNAL TABLE train_acc_ext_i (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE,
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_acc/i.csv" overwrite into table train_acc_ext_i;

SELECT *
FROM train_acc_ext_i
LIMIT 10;

CREATE EXTERNAL TABLE train_gyro_ext_a (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE, 
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_gyro/a.csv" overwrite into table train_gyro_ext_a;

SELECT *
FROM train_gyro_ext_a
LIMIT 10;

CREATE EXTERNAL TABLE train_gyro_ext_b (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE, 
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_gyro/b.csv" overwrite into table train_gyro_ext_b;

SELECT *
FROM train_gyro_ext_b
LIMIT 10;

CREATE EXTERNAL TABLE train_gyro_ext_c (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE, 
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_gyro/c.csv" overwrite into table train_gyro_ext_c;

SELECT *
FROM train_gyro_ext_c
LIMIT 10;

CREATE EXTERNAL TABLE train_gyro_ext_d (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE, 
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_gyro/d.csv" overwrite into table train_gyro_ext_d;

SELECT *
FROM train_gyro_ext_d
LIMIT 10;

CREATE EXTERNAL TABLE train_gyro_ext_e (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE, 
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_gyro/e.csv" overwrite into table train_gyro_ext_e;

SELECT *
FROM train_gyro_ext_e
LIMIT 10;

CREATE EXTERNAL TABLE train_gyro_ext_f (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE, 
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_gyro/f.csv" overwrite into table train_gyro_ext_f;

SELECT *
FROM train_gyro_ext_f
LIMIT 10;

CREATE EXTERNAL TABLE train_gyro_ext_g (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE, 
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_gyro/g.csv" overwrite into table train_gyro_ext_g;

SELECT *
FROM train_gyro_ext_g
LIMIT 10;

CREATE EXTERNAL TABLE train_gyro_ext_h (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE, 
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_gyro/h.csv" overwrite into table train_gyro_ext_h;

SELECT *
FROM train_gyro_ext_h
LIMIT 10;

CREATE EXTERNAL TABLE train_gyro_ext_i (
   Time INT,
   X DOUBLE,
   Y DOUBLE,
   Z DOUBLE, 
   Target STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data local inpath "/clean/train_gyro/i.csv" overwrite into table train_gyro_ext_i;

SELECT *
FROM train_gyro_ext_i
LIMIT 10;

set hive.exec.dynamic.partition=true;                                                                           
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE batch_acc_parq (
   time BIGINT,
   x DOUBLE,
   y DOUBLE,
   z DOUBLE,
   target STRING
)
PARTITIONED BY (
    person STRING
)
STORED AS PARQUET;

INSERT INTO batch_acc_parq
PARTITION(person="a")
SELECT *
FROM batch_acc_ext_a;

INSERT INTO batch_acc_parq
PARTITION(person="b")
SELECT *
FROM batch_acc_ext_b;

INSERT INTO batch_acc_parq
PARTITION(person="c")
SELECT *
FROM batch_acc_ext_c;

INSERT INTO batch_acc_parq
PARTITION(person="d")
SELECT *
FROM batch_acc_ext_d;

INSERT INTO batch_acc_parq
PARTITION(person="e")
SELECT *
FROM batch_acc_ext_e;

INSERT INTO batch_acc_parq
PARTITION(person="f")
SELECT *
FROM batch_acc_ext_f;

INSERT INTO batch_acc_parq
PARTITION(person="g")
SELECT *
FROM batch_acc_ext_g;

INSERT INTO batch_acc_parq
PARTITION(person="h")
SELECT *
FROM batch_acc_ext_h;

INSERT INTO batch_acc_parq
PARTITION(person="i")
SELECT *
FROM batch_acc_ext_i;

SELECT count(*)
FROM batch_acc_parq
GROUP BY person;

CREATE TABLE batch_gyro_parq (
   time BIGINT,
   x DOUBLE,
   y DOUBLE,
   z DOUBLE,
   target STRING
)
PARTITIONED BY (
    person STRING
)
STORED AS PARQUET;

INSERT INTO batch_gyro_parq
PARTITION(person="a")
SELECT *
FROM batch_gyro_ext_a;

INSERT INTO batch_gyro_parq
PARTITION(person="b")
SELECT *
FROM batch_gyro_ext_b;

INSERT INTO batch_gyro_parq
PARTITION(person="c")
SELECT *
FROM batch_gyro_ext_c;

INSERT INTO batch_gyro_parq
PARTITION(person="d")
SELECT *
FROM batch_gyro_ext_d;

INSERT INTO batch_gyro_parq
PARTITION(person="e")
SELECT *
FROM batch_gyro_ext_e;

INSERT INTO batch_gyro_parq
PARTITION(person="f")
SELECT *
FROM batch_gyro_ext_f;

INSERT INTO batch_gyro_parq
PARTITION(person="g")
SELECT *
FROM batch_gyro_ext_g;

INSERT INTO batch_gyro_parq
PARTITION(person="h")
SELECT *
FROM batch_gyro_ext_h;

INSERT INTO batch_gyro_parq
PARTITION(person="i")
SELECT *
FROM batch_gyro_ext_i;

SELECT count(*)
FROM batch_gyro_parq
GROUP BY person;

CREATE TABLE train_acc_parq (
   time BIGINT,
   x DOUBLE,
   y DOUBLE,
   z DOUBLE,
   target STRING
)
PARTITIONED BY (
    person STRING
)
STORED AS PARQUET;

INSERT INTO train_acc_parq
PARTITION(person="a")
SELECT *
FROM train_acc_ext_a;

INSERT INTO train_acc_parq
PARTITION(person="b")
SELECT *
FROM train_acc_ext_b;

INSERT INTO train_acc_parq
PARTITION(person="c")
SELECT *
FROM train_acc_ext_c;

INSERT INTO train_acc_parq
PARTITION(person="d")
SELECT *
FROM train_acc_ext_d;

INSERT INTO train_acc_parq
PARTITION(person="e")
SELECT *
FROM train_acc_ext_e;

INSERT INTO train_acc_parq
PARTITION(person="f")
SELECT *
FROM train_acc_ext_f;

INSERT INTO train_acc_parq
PARTITION(person="g")
SELECT *
FROM train_acc_ext_g;

INSERT INTO train_acc_parq
PARTITION(person="h")
SELECT *
FROM train_acc_ext_h;

INSERT INTO train_acc_parq
PARTITION(person="i")
SELECT *
FROM train_acc_ext_i;

SELECT count(*)
FROM train_acc_parq
GROUP BY person;

CREATE TABLE train_gyro_parq (
   time BIGINT,
   x DOUBLE,
   y DOUBLE,
   z DOUBLE,
   target STRING
)
PARTITIONED BY (
    person STRING
)
STORED AS PARQUET;

INSERT INTO train_gyro_parq
PARTITION(person="a")
SELECT *
FROM train_gyro_ext_a;

INSERT INTO train_gyro_parq
PARTITION(person="b")
SELECT *
FROM train_gyro_ext_b;

INSERT INTO train_gyro_parq
PARTITION(person="c")
SELECT *
FROM train_gyro_ext_c;

INSERT INTO train_gyro_parq
PARTITION(person="d")
SELECT *
FROM train_gyro_ext_d;

INSERT INTO train_gyro_parq
PARTITION(person="e")
SELECT *
FROM train_gyro_ext_e;

INSERT INTO train_gyro_parq
PARTITION(person="f")
SELECT *
FROM train_gyro_ext_f;

INSERT INTO train_gyro_parq
PARTITION(person="g")
SELECT *
FROM train_gyro_ext_g;

INSERT INTO train_gyro_parq
PARTITION(person="h")
SELECT *
FROM train_gyro_ext_h;

INSERT INTO train_gyro_parq
PARTITION(person="i")
SELECT *
FROM train_gyro_ext_i;

SELECT count(*)
FROM train_gyro_parq
GROUP BY person;

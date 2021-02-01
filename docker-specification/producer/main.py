#!/usr/bin/python3

import os
import time
from kafka import KafkaProducer
import kafka.errors
import pandas as pd
import json

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "phone_data"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

acc_df = pd.read_csv("clean/stream_acc.csv", names=["time", "x", "y", "z", "person", "target"], index_col=None).sort_values(by="time", axis=0).reset_index(drop=True)
gyro_df = pd.read_csv("clean/stream_gyro.csv", names=["time", "x", "y", "z", "person", "target"], index_col=None).sort_values(by="time", axis=0).reset_index(drop=True)

begin_time = acc_df.iloc[0]["time"]
end_time = begin_time + 5000
print(begin_time, type(begin_time))

final_time = acc_df.iloc[acc_df.shape[0]-1]["time"]
print(final_time, type(final_time))

start_check = time.time()

while end_time < final_time:

    mask_acc = (acc_df['time'] >= begin_time) & (acc_df['time'] <= end_time)
    mask_gyro = (gyro_df['time'] >= begin_time) & (gyro_df['time'] <= end_time)

    sub_acc = acc_df[mask_acc]
    sub_gyro = gyro_df[mask_gyro]

    print(sub_acc.shape)
    # print(sub_acc.head())

    print(sub_gyro.shape)
    # print(sub_gyro.head())

    if sub_acc.shape[0] < 20 or sub_gyro.shape[0] < 20:
        begin_time = end_time
        end_time = begin_time + 5000
        continue

    for iter_ind, iter_data in sub_acc.iterrows():
        byte_data = bytes(json.dumps(iter_data.to_numpy().tolist()), encoding="utf-8")
        # orig_data = np.array(json.loads(byte_data.decode("utf-8")))
        producer.send(TOPIC, key=bytes("acc", "utf-8"), value=byte_data)

    for iter_ind, iter_data in sub_gyro.iterrows():
        byte_data = bytes(json.dumps(iter_data.to_numpy().tolist()), encoding="utf-8")
        # orig_data = np.array(json.loads(byte_data.decode("utf-8")))
        producer.send(TOPIC, key=bytes("gyro", "utf-8"), value=byte_data)

    begin_time = end_time
    end_time = begin_time + 5000

    time.sleep(15.0 - ((time.time()-start_check) % 15.0))



# for comment in subreddit.stream.comments():
    # producer.send(TOPIC, key=bytes(comment.author.name, 'utf-8'), value=bytes(comment.body, 'utf-8'))
# encoding:utf-8

from confluent_kafka import TopicPartition
from confluent_kakfa_tools import confluent_kafka_producer
if __name__ == '__main__':
    servers = '42.236.61.59:9092'
    while True:
        p = confluent_kafka_producer(topic="domains", servers=servers, timeout=0)
        for data in range(10000):
            p.push(value=data)

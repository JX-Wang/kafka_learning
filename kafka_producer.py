# usr/bin/enc python
# encoing:utf-8
"""
for learing kafka
===================
Author @ wangjunxiong
Date @ 2019/7/19
"""
from kafka import KafkaConsumer, KafkaProducer


class kafka_producer:
    def __init__(self):
        self.bootstrap_sever = "10.245.146.115:9092"
        self.topic = "ddivide6"

    def push(self):
        print "Start"
        try:
            producer = KafkaProducer(bootstrap_servers=[self.bootstrap_sever])  # topic->str brokers->list
        except:
            print "consumer read error"
        print 1
        for i in range(10):
            producer.send(topic=self.topic, value="# However, in production environments the default value of 3 seconds is more suitable as this will help to avoid unnecessary, and potentially expensive, rebalances during application startup.")
            producer.flush()

if __name__ == '__main__':
    kafka_producer().push()



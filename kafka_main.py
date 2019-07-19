# usr/bin/enc python
# encoing:utf-8
"""
for learing kafka
===================
Author @ wangjunxiong
Date @ 2019/7/19
"""
from kafka import KafkaConsumer


class kafka_t:
    def __init__(self):
        self.bootstrap_sever = "10.245.146.107:9092"
        self.topic = "domain_queue"

    def pull(self):
        print "Start"
        try:
            consumer = KafkaConsumer(self.topic, bootstrap_servers=[self.bootstrap_sever])  # topic->str brokers->list
        except:
            print "consumer read error"
        print 1
        for msg in consumer:
            print msg
        print "Done"

if __name__ == '__main__':
    kafka_t().pull()



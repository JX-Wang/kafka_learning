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
    def __init__(self, topic, server_list):
        self.server_list = server_list
        self.topic = topic
        self.partition = 1

    def push(self, values):
        try:
            producer = KafkaProducer(bootstrap_servers=[self.server_list])
            producer.send(topic=self.topic, value=values)
            producer.flush()
        except Exception as e:
            print "E kafka producer send data error:", str(e)


class kafka_consumer:
    def __init__(self, topic, server_list):
        self.bootstrap_sever = "10.245.146.115:9092"
        self.topic = "ddivide6"
        self.partition = 1

    def pull(self):
        try:
            consumer = KafkaConsumer(self.topic, bootstrap_servers=[self.bootstrap_sever])  # topic->str brokers->list
        except:
            print "consumer read error"
        print 1
        for msg in consumer:
            print msg.topic, msg.value


class clean_topic:
    def __init__(self):
        pass

    def clean(self):

if __name__ == '__main__':
    pass



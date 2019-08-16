# usr/bin/enc python
# encoding:utf-8
"""
for learing kafka_broker
===================
Author @ wangjunxiong
Date @ 2019/7/19
"""
from kafka import KafkaConsumer, KafkaProducer
from time import sleep


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
            print "E kafka_broker producer send data error:", str(e)


class kafka_consumer:
    def __init__(self, topic, server_list):
        self.bootstrap_sever = server_list
        self.topic = topic
        self.partition = 1

    def pull(self):
        try:
            consumer = KafkaConsumer(self.topic, bootstrap_servers=[self.bootstrap_sever])  # topic->str brokers->list
        except:
            print "consumer read error"
        for msg in consumer:
            yield msg.topic, msg.value


class kafka_create_topic:
    """The best way for creating topic is using kafka_broker shell but not python shell"""
    def __init__(self):
        pass

    def create(self):
        pass


class clean_topic:
    """the same as create topic"""
    def __init__(self):
        pass

    def clean(self):
        pass


if __name__ == '__main__':
    topic = "ddivide6"
    server = "10.245.146.115:9092"
    msg_content = kafka_consumer(topic=topic, server_list=server).pull()
    while 1:
        try:
            domain = msg_content.next()
            # 这里可以将domain存文件后读取文件进行探测
            # 也可以直接调用domain进行探测
            # 存完文件，或者探测完后，通过while循环再次从生成器中读取发来的的数据
        except Exception as e:
            pass  # 没有next表示broker中没有数据，继续监听即可




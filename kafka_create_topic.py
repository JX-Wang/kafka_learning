# usr/bin/enc python
# encoding:utf-8
"""
for learing confluent_kafka
===================
Author @ wangjunxiong
Date @ 2019/8/21
"""
import pytest

from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource
from confluent_kafka import KafkaException, KafkaError, libversion
import confluent_kafka
import concurrent.futures

from time import sleep


class confluent_kafka_creat_topic(object):
    def __init__(self):
        self.servers = "42.236.61.59:2181"
        ConfigResource()
        pass

    def create(self):
        parma = {
            'bootstrap.servers': self.servers,
            # 'auto_offset_reset': self.auto_offset_reset
        }
        a = AdminClient(conf=parma)
        kw = {
            'operation_timeout': 0,
            'request_timeout': 1000,
            'validate_only': True
        }
        topics = NewTopic(topic="mytopic", num_partitions=1, replication_factor=1)
        print topics

        f = a.create_topics(new_topics=[topics], **kw)
        with pytest.raises(Exception):
            a.create_topics(None)

        with pytest.raises(Exception):
            a.create_topics("mytopic")

        with pytest.raises(Exception):
            a.create_topics(["mytopic"])

        with pytest.raises(Exception):
            a.create_topics([None, "mytopic"])

        with pytest.raises(Exception):
            a.create_topics([None, NewTopic("mytopic", 1, 2)])

        fs = a.create_topics([NewTopic("mytopic", 3, 2)])
        with pytest.raises(KafkaException):
            for f in concurrent.futures.as_completed(iter(fs.values())):
                f.result(timeout=1)

        fs = a.create_topics([NewTopic("mytopic", 3,
                                       replica_assignment=[[10, 11], [0, 1, 2], [15, 20]],
                                       config={"some": "config"})])
        with pytest.raises(KafkaException):
            for f in concurrent.futures.as_completed(iter(fs.values())):
                f.result(timeout=1)

        fs = a.create_topics([NewTopic("mytopic", 3, 2),
                              NewTopic("othertopic", 1, 10),
                              NewTopic("third", 500, 1, config={"more": "config",
                                                                "anint": 13,
                                                                "config2": "val"})],
                             validate_only=True,
                             request_timeout=0.5,
                             operation_timeout=300.1)

        for f in concurrent.futures.as_completed(iter(fs.values())):
            e = f.exception(timeout=1)
            assert isinstance(e, KafkaException)
            assert e.args[0].code() == KafkaError._TIMED_OUT

        with pytest.raises(TypeError):
            a.create_topics([NewTopic("mytopic", 3, 2)],
                            validate_only="maybe")

        with pytest.raises(ValueError):
            a.create_topics([NewTopic("mytopic", 3, 2)],
                            validate_only=False,
                            request_timeout=-5)

        with pytest.raises(ValueError):
            a.create_topics([NewTopic("mytopic", 3, 2)],
                            operation_timeout=-4.12345678)

        with pytest.raises(TypeError):
            a.create_topics([NewTopic("mytopic", 3, 2)],
                            unknown_operation="it is")

        with pytest.raises(TypeError):
            a.create_topics([NewTopic("mytopic", 3, 2,
                                      config=["fails", "because not a dict"])])
        pass


if __name__ == '__main__':
    confluent_kafka_creat_topic().create()
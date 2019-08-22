# encoding : utf-8
"""
Integration result files
========================
Author @ Wangjunxiong
Date @ 2019.8.13
"""

import os
import schedule
import time
ORIGINDICT = 'SOURCE/'
RSTDICT = "RST/"
TIMEOUT = 1200
FREQUENT = 30  # seconds
SERVERS = '42.236.61.59:9092'  # kafka cluster broker config
IP = '10.0.0.0'  # local ip
PORT = '8999'  # port

import hashlib

# my python lib
from confluent_kakfa_tools import confluent_kafka_producer


class IntergrationResultFiles:
    def __init__(self):
        """
        IntergrationResultFiles
        .. py:function: checking the dic has been created or not
        """
        basic_dirs = os.listdir(os.getcwd())
        if RSTDICT not in basic_dirs:
            os.mkdir(RSTDICT[:-1])
        else:
            pass

    def produce(self, filename):
        """
        :param filename:
        :return: None
        .. py.function:
        """
        file_url = "http://{ip}:{port}/file/{file_name}".format(ip=IP, port=str(PORT),
                                                                file_name=filename)  # file url

        with open(RSTDICT+filename) as f:
            domain_data = f.read()
            file_md5 = hashlib.md5(domain_data.encode("utf-8")).hexdigest()

        post_body = {
            "id": filename,
            "time": time.time(),
            "file_url": file_url,
            "file_md5": file_md5
        }

        p = confluent_kafka_producer(topic="post-pkg", servers=SERVERS, timeout=1)
        p.push(value=post_body)

    def monitor(self):
        schedule.every(FREQUENT).seconds.do(self.do)
        while True:
            schedule.run_pending()
            time.sleep(1)

    def gettotalnum(self, filename):
        """
        TODO: resolve total number
        :param filename -> str
        :return: total num -> int
        """
        filename = str(filename)
        if not filename:
            return "Error Empty File Name"
        else:
            pass
        try:
            filename_mirror = filename[::-1]
            file_without_md5 = filename_mirror[filename_mirror.index("_") + 1:]
            total = file_without_md5[:file_without_md5.index("_")][::-1]
            return int(total)  # return total num
        except Exception as e:
            return "Resolve File Name Error", str(e)

    def addfiles(self, dirname):
        """
        :param dirname: second floor dir name
        :return: bool:
        """
        addfilename = RSTDICT + dirname  # Result dict
        datafileslist = os.listdir(ORIGINDICT + dirname)  # source dict

        try:
            f = open(addfilename, "w+")
            tmp_rst = []
            for datafile in datafileslist:
                try:
                    f_tmp = open(ORIGINDICT+dirname+"/"+datafile, 'r')
                    data = f_tmp.read()
                    tmp_rst.append(data)
                except Exception as e:
                    print str(e)
            for d in tmp_rst:
                f.write(d)
                f.write("\n")

            self.produce(dirname)  # kafka producer publish rst json data

            os.system(":> "+ORIGINDICT+"%s/Done" % dirname)  # mark this file, mean this dict has been done
        except Exception as e:
            print e
            return "AddFile Error", str(e)

    def do(self):
        msg = os.listdir(ORIGINDICT)
        for dirname in msg:
            dict_name = ORIGINDICT + dirname
            rst = os.listdir(dict_name)
            if "Done" in rst:  # means this SOURCE has been finished
                continue

            total_num = self.gettotalnum(rst[0])  # get the total files num

            if total_num == len(rst):  # if this task done
                self.addfiles(dirname)  #
            else:
                # open this SOURCE and check time !
                rst.sort(key=lambda x: int(x[:15]))
                final_filename = rst[0]
                timestart = os.path.getctime(dict_name+"/"+final_filename)  # get file create time / not fit for windows_sys
                timenow = time.time()
                if timenow - timestart >= 1200:
                    self.addfiles(dirname)  # SOURCE/00/
                else:
                    pass
        # Done


if __name__ == '__main__':
    I = IntergrationResultFiles()
    I.monitor()
    # bingo
    # I.do()
    # print I.gettotalnum("1566302883_id_01_3_md5")

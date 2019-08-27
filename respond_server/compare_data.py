# encoding:utf-8

from read_config import *
import time
import os
import hashlib
import schedule
from Logger import Logger
from collections import defaultdict
from confluent_kakfa_tools import confluent_kafka_producer
from db_manage.data_base import MySQL
from db_manage.mysql_config import SOURCE_CONFIG_LOCAL as SOURCE_CONFIG
show_terminal = read_log_show()
logger = Logger(file_path='./query_log/', show_terminal=show_terminal)  # 日志配置
servers = read_confluent_kafka()  # kafka服务器的地址
ip, port = read_http_server()  # 响应web服务器的ip和端口
FREQUENT = 10
unres_address = './domain_dns_data/sec_merged'  # 待处理数据存放位置
res_address = './domain_dns_data/sec_compared'  # 处理完的数据存放位置


class compare_data:
    def __init__(self):
        pass

    def monitor(self):
        schedule.every(FREQUENT).seconds.do(self.start)
        while True:
            schedule.run_pending()
            time.sleep(1)

    def read_file(self, file_address):
        """
        读取文件
        """
        domain_ns = defaultdict(list)
        with open(file_address, 'r') as fr:
            domain_data = fr.readlines()
            fr.close()
        for data in domain_data:
            a = data.split('\t')
            b = [x.strip() for x in a]
            domain_ns.setdefault(b[0],[]).append(b[2])
            domain_ns.setdefault(b[0],[]).append(b[3])
        return domain_ns

    def read_domain_ns(self, db):
        """
        读取数据库
        """
        mysql_data = defaultdict(list)
        sql ='SELECT domain,ns,flag FROM domain_ns'
        try:
           db.query(sql)
           data = db.fetch_all_rows()
        except Exception as e:
            return "read mysql Error", str(e)
        for i in data:
            domain = i['domain'].encode('utf-8')
            ns = i['ns'].encode('utf-8')
            flag = i['flag'].encode('utf-8')
            mysql_data.setdefault(domain,[]).append(ns)
            mysql_data.setdefault(domain,[]).append(flag)
        return mysql_data

    def read_tld(self, db):
        data_list = []
        sql = 'SELECT tld,server_name FROM tld_ns_zone'
        try:
            db.query(sql)
            data = db.fetch_all_rows()
        except Exception as e:
            return "read tld Error", str(e)
        for i in data:
            tld = i['tld'].encode('utf-8')
            ns = str(i['server_name'])
            ns_list = ns.split(';')
            ns = ','.join(ns_list)
            flag = '1'
            data_list.append((tld,ns,flag))
        return data_list

    def update_mysql(self, db,update_list):
        """
        更新数据库
        """
        sql = 'INSERT INTO domain_ns (domain,ns,flag) VALUES (%s,%s,%s) \
                ON DUPLICATE KEY UPDATE domain=values (domain),ns =values (ns),flag=VALUES (flag)'
        try:
            db.update_many(sql, update_list)
            logger.logger.info("更新数据库成功")
        except:
            logger.logger.error("更新数据库失败")

    def get_file_name(self, unres_address, res_address):
        """
        获得需要处理的文件的名字
        """
        unres_list = os.listdir(unres_address)
        res_list = os.listdir(res_address)
        file_list = []
        for ads in unres_list:
            res_ads = ads + '_res'
            flag = 0
            for i in res_list:
                if i == res_ads:
                    flag = 1
                    break
            if flag == 0:
                file_list.append(ads)
        return file_list

    def produce(self, dir_name):
        """
        :param filename:
        :return: bool:
        """
        if "update" in dir_name:
            file_name = dir_name.replace(res_address, '')
            task_id = file_name.replace("_update", '')

            file_url = "http://{ip}:{port}/update_/{file_name}".format(ip=ip, port=str(port),
                                                                    file_name='sec_compared/'+task_id+'_update')

            print file_url

            with open(dir_name) as f:
                domain_data = f.read()
                file_md5 = hashlib.md5(domain_data.encode("utf-8")).hexdigest()
    
            post_body = {
                "id": task_id,
                "time": time.time(),
                "file_url": file_url,
                "file_md5": file_md5,
                "task_type": "sec"
            }

            try:
                p = confluent_kafka_producer(topic="post-pkg", servers=servers, timeout=1)
                p.push(value=post_body)
            except Exception as e:
                logger.logger.error("Sec_update Producer Error", str(e))
                return False
        elif "res" in dir_name:
            file_name = dir_name.replace(res_address, '')
            task_id = file_name.replace("_res", '')

            file_url = "http://{ip}:{port}/file_/{file_name}".format(ip=ip, port=str(port),
                                                                    file_name='sec_compared/' + task_id + '_res')

            print file_url

            with open(dir_name) as f:
                domain_data = f.read()
                file_md5 = hashlib.md5(domain_data.encode("utf-8")).hexdigest()

            post_body = {
                "id": task_id,
                "time": time.time(),
                "file_url": file_url,
                "file_md5": file_md5,
                "task_type": "sec"
            }

            try:
                p = confluent_kafka_producer(topic="post-pkg", servers=servers, timeout=1)
                p.push(value=post_body)
            except Exception as e:
                logger.logger.error("Sec_res Producer Error", str(e))
                return False
        return True

    def start(self):
        try:
            db = MySQL(SOURCE_CONFIG)
        except:
            logger.logger.error("数据库异常：获取域名失败")
            return
        tld_list = self.read_tld(db)
        mysql_data = self.read_domain_ns(db)
        file_list = self.get_file_name(unres_address, res_address)
        for i in file_list:
            update_list = []
            res_list = []
            file_address = unres_address + '/' + i
            file_data = self.read_file(file_address)
            for k, v in file_data.items():
                if k not in mysql_data.keys():
                    update_list.append((k, v[0], v[1]))
                    res_list.append((k, v[0], v[1]))
                else:
                    file_flag = int(v[1])
                    mysql = mysql_data[k]
                    ns = mysql[0]
                    flag = mysql[1]
                    mysql_flag = int(mysql[1])
                    if file_flag > mysql_flag:
                        print k
                        update_list.append((k, v[0], v[1]))
                        res_list.append((k, v[0], v[1]))
                    else:
                        res_list.append((k, ns, flag))
            self.update_mysql(db, update_list)
            update_file_address = res_address + '/' + i + '_update'
            with open(update_file_address, 'w') as fw:
                for s in update_list:
                    if s[2] == '0' or s[2] == '-1':
                        pass
                    else:
                        fw.write(s[0] + '\t' + 'NS' + '\t' + s[1] + '\t' + s[2] + '\n')
            self.produce(update_file_address)
            res_file_address = res_address + '/' + i + '_res'
            with open(res_file_address, 'w') as fw:
                for i in res_list:
                    if i[2] == '0' or i[2] == '-1':
                        pass
                    else:
                        fw.write(i[0] + '\t' + 'NS' + '\t' + i[1] + '\t' + i[2] + '\n')
                for t in tld_list:
                    if t[2] == '0' or t[2] == '-1':
                        pass
                    else:
                        fw.write(t[0] + '\t' + 'NS' + '\t' + t[1] + '\t' + t[2] + '\n')
            self.produce(res_file_address)


if __name__ == '__main__':
    compare_data().start()
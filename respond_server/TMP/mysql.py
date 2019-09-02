import MySQLdb

def get_ns_history_record():
    ns_db = "wud"
    ns_table = "domain"

    mysql_connect_parma = {
        "user": "root",
        "port": 3306,
        "host": "39.106.165.57",
        "passwd": "949501wud",
        "db": ns_db,
        "charset": "utf8"
    }

    try:
        db = MySQLdb.connect(**mysql_connect_parma)
        cursor = db.cursor()
        # logger.logger.info("Mysql Connect Success!")
    except Exception as e:
        print e
        # logger.logger.error("Mysql Connect Success Error!", str(e))
        return
    with open("sld.txt", "r") as f:
        domains = f.readlines()
    count = 0
    for domain in domains:
        domain = domain.strip()
        count += 1
        get_data_sql = """INSERT INTO {TABLE} (domain, domain_ns) VALUES ("{domain}", '--')""".format(TABLE=ns_table, domain=domain)
        try:
            cursor.execute(get_data_sql)
            # logger.logger.info("Fetch Domains NS Data Success!")
        except Exception as e:
            print e
        if count == 1500:
            db.commit()
            print "commit it"
        else:
            pass
        print count
    db.commit()


if __name__ == '__main__':
    get_ns_history_record()
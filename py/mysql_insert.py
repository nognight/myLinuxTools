import os, re, sys, datetime, pymysql, logging

now = datetime.datetime.now()
time = now
yesterday = (time + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
the_day_before_yesterday = (time + datetime.timedelta(days=-2)).strftime('%Y-%m-%d')

db_config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'ahst',
    'password': 'Ahst@123ahst',
    'db': 'analysis',
    'charset': 'utf8',
    'cursorclass': pymysql.cursors.DictCursor
}

once_insert_mount = 20000

base_file_home = '/home/ftp/daliy/'

file_tuple = ('iphone/030_iphone_' + yesterday + '.txt', 'android/030_android_' + yesterday + '.txt')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='/home/ftp/insert.log',
                    filemode='a')


def split_by_space(line_str):
    return list(line_str.split("\t"))
    # return list(filter(None,line_str.split("\t"))) 


def insert_into_db(values):
    print('start to connect to db')
    try:
        conn = pymysql.connect(**db_config)
        # 创建游标
        cur = conn.cursor()
        # ['17681225857', '00304', '0', 'OK', '2019-03-03 22:07:40', '139.214.254.199', '301', 'iphone_c', '4G00', '5.94', 'null\n']
        list = ['17681225857', '00304', '0', 'OK', '2019-03-03 22:07:40', '139.214.254.199', '301', 'iphone_c', '4G00',
                '5.94', 'null\n']
        #   `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
        #   `user_mobile` varchar(20) NOT NULL DEFAULT '' COMMENT 'userMobile',
        #   `biz_id` varchar(50) NOT NULL DEFAULT '' COMMENT 'bizId',
        #   `fail_reson` varchar(200) NOT NULL DEFAULT '' COMMENT 'failReson',
        #   `query_time` varchar(50) NOT NULL DEFAULT '' COMMENT 'queryTime',
        #   `result` varchar(2) NOT NULL DEFAULT '' COMMENT 'result',
        #   `user_ip` varchar(50) NOT NULL DEFAULT '' COMMENT 'userIp',
        #   `city_id` varchar(50) NOT NULL DEFAULT '' COMMENT 'cityId',
        #   `application` varchar(50) NOT NULL DEFAULT '' COMMENT 'application',
        #   `brand_id` varchar(50) NOT NULL DEFAULT '' COMMENT 'brandId',
        #   `version_id` varchar(50) NOT NULL DEFAULT '' COMMENT 'versionId',
        #   `net_id` varchar(50) NOT NULL DEFAULT '' COMMENT 'net_id',
        #   `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'createTime',
        sql = "insert into log_phone_busi_info(user_mobile,biz_id,fail_reson,query_time,result,user_ip,city_id,application,brand_id,version_id,net_id) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        print('db start to executemany ' + str(len(values)))
        logging.info('db start to executemany ' + str(len(values)))
        ret = cur.executemany(sql, values)
        cur.close()
        conn.commit()
        conn.close()
        print('db success ' + str(ret))
        logging.info('db success ' + str(ret))
    except Exception:
        print(Exception)
        logging.error(Exception)


def read_line(file_path):
    values = []
    count = 0
    print('start to do read_line ' + str(file_path))
    logging.info('start to do read_line ' + str(file_path))
    for line in open(file_path, "rb"):
        list = []
        try:
            list = split_by_space(line.decode('utf8'))
        except:
            list = split_by_space(line.decode('GBK'))

        try:
            item = (
            (list[0], list[1], list[3], list[4], list[2], list[5], list[6], list[7], list[8], list[9], list[10]))
            values.append(item)
        except Exception:
            print(Exception)
            logging.error(Exception)

        if len(values) >= once_insert_mount:
            print(str(len(values)))
            logging.info('start to do insert_into_db ' + str(len(values)))
            count = count + len(values)
            insert_into_db(values)
            print('end do insert_into_db count ' + count)
            logging.info('end do insert_into_db count ' + count)
            values.clear()

    insert_into_db(values)
    count = count + len(values)
    print('end all do insert_into_db count ' + count)
    logging.info('end all do insert_into_db count ' + count)


def main():
    read_line(base_file_home + file_tuple[0])
    read_line(base_file_home + file_tuple[1])


if __name__ == '__main__':
    main()

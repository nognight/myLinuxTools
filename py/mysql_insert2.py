import os, re, sys, datetime, pymysql, logging

now = datetime.datetime.now()
time = now
yesterday = (time + datetime.timedelta(days=-1)).strftime('%Y%m%d')
the_day_before_yesterday = (time + datetime.timedelta(days=-2)).strftime('%Y%m%d')

db_config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'ahst',
    'password': 'Ahst@123ahst',
    'db': 'analysis',
    'charset': 'utf8',
    'cursorclass': pymysql.cursors.DictCursor
}

once_insert_mount = 50000

base_file_home = '/home/ftp/daliy/'

file_tuple = ('pvuv/030_wap_firstpage_' + yesterday + '.txt',
              'appNew/030_phone_newuser_add_' + yesterday + '.txt',
              'usermon/030_usermon_add_' + yesterday + '.txt')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='/home/ftp/insert2.log',
                    filemode='a')


def split_by_space(line_str):
    return list(line_str.split("|"))
    # return list(filter(None,line_str.split("\t"))) 


def insert_into_db(values, type):
    print('start to connect to db')
    try:
        conn = pymysql.connect(**db_config)
        # 创建游标
        cur = conn.cursor()
        sql = [
            "insert into log_wap_firstpage(visit_time,user_phone,province_code,city_code,entry_name,type_name,menu_code,menu_name,visit_url,client_ver,client_type) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            "insert into log_phone_newuser(user_phone,user_sex,user_birthday,net_type,package_type,package_id,in_net_time,treaty_start_time,treaty_end_time,state,is__r_h,is_black_list,pay_type,phone_type,arpu,mouthly_flow_use__m,speech_amount_minute) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            "insert into log_usermon(user_phone,user_channel,city_code) values(%s,%s,%s)"]
        print('db start to executemany ' + str(len(values)))
        logging.info('db start to executemany ' + str(len(values)))
        ret = cur.executemany(sql[type], values)
        cur.close()
        conn.commit()
        conn.close()
        print('db success ' + str(ret))
        logging.info('db success ' + str(ret))
    except Exception:
        print('e '+Exception)
        logging.error('e '+Exception)


def read_line(file_path, type):
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
            item = tuple(list)
            values.append(item)
        except Exception:
            print(Exception)
            logging.error(Exception)

        if len(values) >= once_insert_mount:
            print(str(len(values)))
            logging.info('start to do insert_into_db ' + str(len(values)))
            count = count + len(values)
            insert_into_db(values,type)
            print('end do insert_into_db count ' + str(count))
            logging.info('end do insert_into_db count ' + str(count))
            values.clear()

    insert_into_db(values,type)
    count = count + len(values)
    print('end all do insert_into_db count ' + str(count))
    logging.info('end all do insert_into_db count ' + str(count))


def main():
    read_line(base_file_home + file_tuple[0], 0)
    read_line(base_file_home + file_tuple[1],1)
    read_line(base_file_home + file_tuple[2],2)


if __name__ == '__main__':
    main()

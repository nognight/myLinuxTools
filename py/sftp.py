#!/usr/bin/python2
# -*- coding: utf-8 -*-

'''
Created on 2016年8月27日
modified by ycc@180111

@author: yyk
'''
import pymysql
import datetime, time, logging, os, os.path, sys, shutil
from phone_util import is_valid_phone
from SFTP import *
from Alarm import notify_mail, notify_sms

TODAY = time.strftime('%Y%m%d', time.localtime(time.time()))

WORK_PATH = '/opt/lljy/shbank/work/'
BACKUP_PATH = '/opt/lljy/shbank/backup/'
REPORT_PATH = '/opt/lljy/shbank/report/'

if not os.path.exists(WORK_PATH): os.makedirs(WORK_PATH)
if not os.path.exists(BACKUP_PATH): os.makedirs(BACKUP_PATH)
if not os.path.exists(REPORT_PATH): os.makedirs(REPORT_PATH)

# 日志配置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='/opt/lljy/logs/shbank.log',
                    filemode='a')

# MySQL配置
db_cfg = {
    'host': '192.168.77.20',
    'port': 3306,
    'user': 'lljy',
    'password': 'lljy123',
    'db': 'lljy',
    'charset': 'utf8',
    'cursorclass': pymysql.cursors.DictCursor,
}
'''
#MySQL配置
db_cfg = {
          'host':'121.42.200.153',
          'port':3306,
          'user':'lljy',
          'password':'lljy123',
          'db':'lljy',
          'charset':'utf8',
          'cursorclass':pymysql.cursors.DictCursor,
          }
'''


def get_yesterday():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    return yesterday


DEFAULT_EXCH_DATE = get_yesterday().strftime('%Y%m%d')


def ymd_2_datetime(ymd):
    return ymd[0:4] + '-' + ymd[4:6] + '-' + ymd[6:8] + ' 00:00:00'  # 数据库时间格式


def is_vip(points, res):
    return points / res < 16  # 白金卡和金卡20：1，钻石卡15：1，判断是否为钻石卡，统计报表需求，暂不实现


def exch(exch_date=DEFAULT_EXCH_DATE):
    sshd = ssh_connect('10.95.209.125', 'yanhuang', 'Yanhuang#123')
    sftpd = sftp_open(sshd)

    this_month = exch_date[0:6]  # 报表目前是月报
    exch_filename = 'LTEXCH.' + exch_date + '.txt'  # 积分兑换文件
    exch_r_filename = 'LTEXCH-R.' + exch_date + '.txt'  # 回盘文件
    report_stat_filename = 'LTEXCH-STAT.' + this_month + '.txt'  # 统计报表，按金卡/白金卡/钻石卡统计总量，暂不实现
    report_list_filename = 'LTEXCH-LIST.' + this_month + '.txt'  # 明细报表，原始记录+结果，可直接拷贝至Excel

    work_exch_file = WORK_PATH + exch_filename
    work_exch_r_file = WORK_PATH + exch_r_filename
    backup_exch_file = BACKUP_PATH + exch_filename
    backup_exch_r_file = BACKUP_PATH + exch_r_filename
    report_stat_file = REPORT_PATH + report_stat_filename
    report_list_file = REPORT_PATH + report_list_filename
    remote_exch_file = '/home/bankofsh/Bank/User_Points/' + exch_filename
    remote_exch_r_file = '/home/bankofsh/Unicom/User_Points/' + exch_r_filename

    logging.info("上海银行积分兑换任务开始,兑换日期： %s" % (exch_date))

    # 优先判断本地文件，不存在则FTP获取
    if not os.path.exists(work_exch_file):
        try:
            sftp_get(sftpd, remote_exch_file, work_exch_file)
            logging.info('下载积分兑换文件成功： %s' % (work_exch_file))
        except Exception, e:
            logging.error('本地无法找到积分兑换文件且下载失败，退出程序： %s' % (e))
            notify_mail('流量经营平台能力网关告警', '无法找到上海银行积分兑换文件，下载失败或本地不存在: File=%s' % (exch_filename))
            exit(-1)  # 退出
    else:
        logging.info('使用本地积分兑换文件： %s' % (work_exch_file))

    # 读文件
    try:
        exch_file_object = open(work_exch_file, "r")
        exch_r_file_object = open(work_exch_r_file, "a+")
        report_list_file_object = open(report_list_file, "a+")
        records = exch_file_object.readlines()
    except Exception, e:
        logging.error('打开文件异常，退出程序: %s' % (e))
        notify_mail('流量经营平台能力网关告警', '无法打开上海银行积分兑换文件: File=%s' % (work_exch_file))
        exit(-1)  # 退出

    # 解析并入库
    try:
        conn = pymysql.connect(**db_cfg)
        conn.autocommit(True)
        cur = conn.cursor()
        for record in records:
            logging.debug('读取一行记录： %s' % (record))
            out = -1
            try:
                fields = record.split("|")
                rec_type = fields[0]
                if rec_type == 'D':  # 数据行
                    exch_date, card, name, phone, points, res_amount, res_type, res_unit, \
                    product_id, order_no, valid_start, valid_month, description, end_line = fields[1:15]
                elif rec_type == 'T':  # 结束行
                    logging.debug('结束行')
                    file_date, total_rec, total_res, end_line = fields[1:5]
                    logging.info('解析结束，总记录数：%s，总流量：%s' % (total_rec, total_res))
                    break
                else:
                    logging.error('记录格式无效： %s' % (record))
                    continue
                if is_valid_phone(phone):  # 根据上海联通HLR号段表判定是否上海联通用户，若否直接失败，写回盘文件
                    exch_date_db = ymd_2_datetime(exch_date)
                    valid_start_db = ymd_2_datetime(valid_start)
                    res_type = 2 if res_type == 11 else 1
                    # res_unit
                    output = 0
                    logging.debug('调用存储过程')
                    cur.callproc('sp_shyh_add_ex_record',
                                 (phone, exch_date_db, card, name, int(points), res_type, int(res_amount), \
                                  product_id, order_no, valid_start_db, int(valid_month), description, output))
                    cur.execute('select @_sp_shyh_add_ex_record_12')
                    data = cur.fetchall()
                    if data:
                        for rec in data:
                            out = rec['@_sp_shyh_add_ex_record_12']
                            if (out != 0):
                                logging.error('存储过程失败： %s' % (record))
                            else:
                                notify_sms(phone, phone + '用户，您上海银行积分换购流量已入账，详情可登录wallet.ishwap.com【流量荟】')
                    else:
                        logging.error('获取存储过程结果失败： %s' % (record))
                else:
                    logging.debug('非上海联通号段： %s' % (phone))  # 很多移动兑换，设置低级别
            except Exception, e:
                logging.error('MySQL异常: %s, %s' % (e, record))
            # 根据结果写回盘文件
            result = 2 if out == 0 else 3  # 2-兑换完成，3-兑换失败
            record_r = '|'.join([exch_date, order_no, product_id, str(result), "  "]) + "\r\n"
            try:
                exch_r_file_object.write(record_r)
            except Exception, e:
                logging.error('写回盘文件失败: %s, %s' % (e, record_r))
                notify_mail('流量经营平台能力网关告警', '写上海银行积分兑换回盘文件失败: File=%s' % (work_exch_r_file))
            # 根据结果写明细报表
            result_desc = '成功' if out == 0 else '失败'
            record_report = record[:-87] + result_desc + "\r\n"  # 去掉原始记录最后一列86个补充空格
            try:
                report_list_file_object.write(record_report)
            except Exception, e:
                logging.error('写明细报表失败: %s, %s' % (e, record_report))
    except Exception, e:
        logging.error('发生异常: %s' % (e))
    finally:
        exch_file_object.close()
        exch_r_file_object.close()
        report_list_file_object.close()
        cur.close()
        conn.commit()
        conn.close()

    # 上传回盘文件并删除原始文件，避免重复执行
    upload_flag = 0
    try:
        sftp_put(sftpd, work_exch_r_file, remote_exch_r_file)
        upload_flag = 1
        sftp_remove(sftpd, remote_exch_file)
    except Exception, e:
        if upload_flag == 1:
            logging.error('删除原始文件失败： %s' % (e))
        else:
            logging.error('上传回盘文件失败： %s' % (e))
            notify_mail('流量经营平台能力网关告警', '上传上海银行积分兑换回盘文件失败: File=%s' % (work_exch_r_file))
    finally:
        sftp_close(sftpd)
        ssh_close(sshd)

    # 备份
    try:
        shutil.move(work_exch_file, backup_exch_file)
        shutil.move(work_exch_r_file, backup_exch_r_file)
    except Exception, e:
        logging.error('移动文件到备份文件夹失败: %s' % (e))
        logging.info("上海银行积分兑换任务结束")

        # 上传到亚信ftp

        # 公网测试主机：120.52.49.67:8122
        # 上海银行
        # 账号fshbankuser
        # 密码fshbankuser123
        # 上传路径/upload
        # 下载路径/download
        logging.info("上海银行积分上传到亚信")
        yx_sshd = ssh_connect('120.52.49.67:8122', 'fshbankuser', 'fshbankuser123')
        yx_sftpd = sftp_open(sshd)
        yx_upload_path = '/upload'
        yx_download_path = '/download'
        sftp_put(sftpd, work_exch_file, yx_upload_path + exch_filename)
        logging.info("上海银行积分上传成功")


def usage():
    print
    'job_exch usage:'
    print
    '[exch_date]: Date format "yyyymmdd", default=today.'


if len(sys.argv) == 1:
    exch()
elif len(sys.argv) == 2:
    exch_date = sys.argv[1]
    exch(exch_date)
else:
    usage()
    logging.error('无效命令！')
    exit(1)

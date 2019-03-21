#!/usr/bin/python
# -*- coding: utf-8 -*-
import ftplib
import os
import socket
import datetime, logging, smtplib
from email.mime.text import MIMEText
from email.header import Header

# 第三方 SMTP 服务
mail_host = "smtp.163.com"  # 设置服务器
mail_user = "nognight"  # 用户名
mail_pass = "fate99296"  # 口令

# 地址
HOST = '132.38.0.157'
DIRN = './'

# 目录
PVUV_DIR = '/home/ftp/daliy/pvuv/'
APPNEW_DIR = '/home/ftp/daliy/appNew/'
USERMON_DIR = '/home/ftp/daliy/usermon/'

# 日志配置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='/home/ftp/ftp2.log',
                    filemode='a')

# 时间
now = datetime.datetime.now()
time = now

REPORT = ''


def get_date():
    global today, yesterday, day_before_yesterday, first_day, last_month_first_day, last_month_today
    print('get_date start')
    today = time.strftime('%Y%m%d')
    print(today)
    yesterday = (time + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    print(yesterday)
    day_before_yesterday = (time + datetime.timedelta(days=-2)).strftime('%Y%m%d')
    print(day_before_yesterday)
    first_day = time.replace(day=1).strftime('%Y%m%d')
    print(first_day)
    last_month_first_day = (time.replace(day=1) - datetime.timedelta(1)).replace(day=1).strftime('%Y%m%d')
    print(last_month_first_day)

    day = get_last_month_today(time.day)

    last_month_today = (time.replace(day=1) - datetime.timedelta(1)).replace(
        day=day).strftime('%Y%m%d')
    print(last_month_today)
    return 0


# 上月的今天，对月份日期进行特殊处理
def get_last_month_today(today_day):
    day = today_day
    if time.day == 31 and time.month in (1, 3, 5, 7, 8, 12):
        day = 30
    if time.month == 3 and time.day > 28:
        if time.year % 4 == 0:
            day = 29
        else:
            day = 28
    return day


def send_email(report):
    sender = 'nognight@163.com'
    receivers = ['nognight@163.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

    message = MIMEText('ftp2每日报告邮件\n ' + report, 'plain', 'utf-8')
    message['From'] = Header('nognight@163.com', 'utf-8')
    message['To'] = Header('nognight@163.com', 'utf-8')

    subject = 'ftp2每日报告邮件'
    message['Subject'] = Header(subject, 'utf-8')

    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        logging.info("邮件发送成功")
    except smtplib.SMTPException as e:
        logging.error("Error: 无法发送邮件")
        logging.exception(e)


def download_file(ftp, remotepath, localpath):
    bufsize = 1024  # 设置缓冲块大小
    global REPORT
    try:
        fp = open(localpath, 'wb')  # 以写模式在本地打开文件
        ftp.retrbinary('RETR ' + remotepath, fp.write, bufsize)  # 接收服务器上文件并写入本地文件
        ftp.set_debuglevel(0)  # 关闭调试
        fp.close()  # 关闭文件
        logging.info("***finish ftp download %s" % localpath)
        REPORT = REPORT + '\n ***finish ftp download ' + localpath
    except Exception as e:
        logging.error("download_file Exception")
        logging.exception(e)
        REPORT = REPORT + '\n download_file Exception ' + localpath


def main():
    global REPORT
    try:
        f = ftplib.FTP(HOST)
    except (socket.error, socket.gaierror):
        print('ERROR:cannot reach " %s"' % HOST)
        logging.error("***Connected to host e %s" % (e))
        return
    print('***Connected to host "%s"' % HOST)
    logging.info("***Connected to host： %s" % HOST)
    logging.info("***login date %s" % today)
    print(':login  " %s"' % HOST)
    try:
        f.login(user='ecs030', passwd='tJ2rC4/uq')
        logging.info("***login date %s" % today)
        print(':login  " %s"' % HOST)
    except ftplib.error_perm:
        print('ERROR:login  " %s"' % HOST)
        logging.error("***ERROR:login e %s" % (e))

        f.quit()
        return

    print('cwd  " %s"' % HOST)
    try:
        f.cwd(DIRN)

        pvuvFileName = '030_wap_firstpage_' + yesterday + '.txt'
        appNewFileName = '030_phone_newuser_' + yesterday + '.txt'
        usermonFileName = '030_usermon_' + yesterday + '.txt'

        download_file(f, './' + pvuvFileName, PVUV_DIR + pvuvFileName)
        download_file(f, './' + appNewFileName, APPNEW_DIR + appNewFileName)
        download_file(f, './' + usermonFileName, USERMON_DIR + usermonFileName)

        os.system('chgrp -R ftp /home/ftp')
        os.system('chown -R ftp:ftp /home/ftp/')

        f.quit()

        logging.info("***finish ftp work")

    except ftplib.error_perm:
        print('ERROR:downloadfile  " %s"' % HOST)
        logging.error("***ERROR:downloadfile e %s" % (e))
        REPORT = REPORT + '\n ***ERROR:downloadfile e '
        f.quit()
        return
    return


if __name__ == '__main__':
    get_date()
    main()
    send_email(REPORT)

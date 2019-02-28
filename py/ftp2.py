#!/usr/bin/python
# -*- coding: utf-8 -*-
import ftplib
import os
import socket
import datetime,logging

#地址
HOST = '132.38.0.157'
DIRN = './'

#目录
PVUV_DIR='/home/ftp/daliy/pvuv/'
APPNEW_DIR='/home/ftp/daliy/appNew/'
USERMON_DIR='/home/ftp/daliy/usermon/'

# 日志配置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='/home/ftp/ftp2.log',
                    filemode='a')

# 时间
now = datetime.datetime.now()
time = now

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

def downloadfile(ftp, remotepath, localpath):
    bufsize = 1024                #设置缓冲块大小
    fp = open(localpath,'wb')     #以写模式在本地打开文件
    ftp.retrbinary('RETR ' + remotepath, fp.write, bufsize) #接收服务器上文件并写入本地文件
    ftp.set_debuglevel(0)         #关闭调试
    fp.close()                    #关闭文件
    logging.info("***finish ftp download %s" % localpath)


def main():
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
        f.login(user='ecs030',passwd='tJ2rC4/uq')
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
    
        pvuvFileName='030_wap_firstpage_'+yesterday+'.txt'
        appNewFileName='030_phone_newuser_'+yesterday+'.txt'
        usermonFileName='030_usermon_'+yesterday+'.txt'
       

        downloadfile(f,'./'+pvuvFileName,PVUV_DIR+pvuvFileName)
        downloadfile(f,'./'+appNewFileName,APPNEW_DIR+appNewFileName)
        downloadfile(f,'./'+appNewFileName,USERMON_DIR+usermonFileName)
        

        os.system('chgrp -R ftp /home/ftp')
        os.system('chown -R ftp:ftp /home/ftp/')

        logging.info("***finish ftp work")
       

        f.quit()

    except ftplib.error_perm:
        print('ERROR:downloadfile  " %s"' % HOST)
        logging.error("***ERROR:downloadfile e %s" % (e))
        f.quit()
        return
    return


if __name__ == '__main__':
    get_date()
    main()

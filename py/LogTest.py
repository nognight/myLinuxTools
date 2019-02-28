# encoding: utf-8
# Created by HiWin10 on 2017/6/22.
import os, re, sys, datetime, pymysql, paramiko

DATE_TIME = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
LOG_PATH = './access.log' + DATE_TIME
#LOG_PATH = './access.log20170623'
RECORD_PATH = './record.log'

patternPath = re.compile(r'GET\s*(.*)\s*HTTP')
pathWithoutParam = re.compile(r'\s*(.*)\s*\?')
pathWithNetNum = re.compile(r'\s*(.*)\s*&x-up-calling-line-id')

requestPathsWithoutParam = {
    'entry': '/entry.html',  # 权益专区主页面

    'popu': '/popu.html',  # 权益专区列表页面
    'popu_wx': '/popu_wx.html',  # 权益专区列表页面
    'xinyi_index': '/xinyi_index.html',  # 心意卡页面
    'fudaillh': '/fudaillh.html',  # 福袋页面
    'zhekou_buy': '/zhekou_buy.html',  # 折扣卡页面
    'wedact': '/wedact.html',  # 星期三
    'zhekou_buy': '/zhekou_buy.html'

}
requestPathsWithParam = {
    'entry2': '/entry.html?source=2',
    'entry3': '/entry.html?source=3'
}

dbConfig = {
    'host': '',
    'port': '',
    'user': '',
    'password': '',
    'db': 'utf8mb4',
    'charset': '',
    'cursorclass': pymysql.cursors.DictCursor
}


# 取日志
def getlog():
    # command=r'scp root@192.168.77.5:/usr/local/nginx/logs/access.log'+DATE_TIME+'.gz /root/daliylog/'
    # passwd=r'E!#5swhN'
    # r = os.popen(command,'r').read()
    # print(r)
    # if str(r).__contains__('password'):
    #     os.popen(passwd,'r')
    #     return 0
    # return -1

    t = paramiko.Transport(('192.168.77.5', 22))
    t.connect(username='root', password='E!#5swhN')
    sftp = paramiko.SFTPClient.from_transport(t)
    remotepath = '/usr/local/nginx/logs/access.log' + DATE_TIME + '.gz'
    localpath = '/root/daliylog/access.log' + DATE_TIME + '.gz'
    sftp.get(remotepath, localpath)
    t.close()
    command = 'gunzip ./access.log' + DATE_TIME + '.gz'
    os.popen(command, 'r')
    return 0


# 特殊处理;jsessionid=xxxxx
def handle(s):
    pathWithJse = re.compile(r'html\s*(.*)\s*\?')
    temp = pathWithJse.search(s)
    if temp:
        s = s.replace(temp.group(1), '')
        # print(s)
        return s
    return s


# 获取ip
def getIp(s):
    lineArray = s.split()
    if lineArray:
        ip = lineArray[0]
        #print(ip)
    return ip


# 获取无参记录
def getWithoutParam(s):
    pvArray = {}
    uvArray = {}
    uvList = []
    f = open(s, 'r')
    for line in f:
        ip = getIp(line)
        mPath = patternPath.search(line)
        if mPath:
            path = mPath.group(1).strip()
            path = handle(path)
            mPath = pathWithoutParam.search(path)
            if mPath:
                path = mPath.group(1).strip()

                if path.__contains__('html'):
                    if path in pvArray:
                        count = pvArray[path] + 1
                        pvArray[path] = count
                    else:
                        pvArray[path] = 1

                    if path + ip not in uvList:
                        uvList.append(path + ip)
                        if path in uvArray:
                            count = uvArray[path] + 1
                            uvArray[path] = count
                        else:
                            uvArray[path] = 1


    f.close()
    return (pvArray, uvArray)


# 获取有渠道参记录
def getWithSource(s):
    countArray = {}
    f = open(s, 'r')
    for line in f:
        mPath = patternPath.search(line)
        if mPath:
            path = mPath.group(1).strip()
            path = handle(path)
            mPath = pathWithNetNum.search(path)
            if mPath:
                path = mPath.group(1).strip()

                if path.__contains__('html'):
                    if path in countArray:
                        count = countArray[path] + 1
                        # print(path + '=' + str(count))
                        countArray[path] = count
                    else:
                        countArray[path] = 1
                        # print(path + '=1')
            else:
                if path.__contains__('html'):
                    if path in countArray:
                        count = countArray[path] + 1
                        # print(path + '=' + str(count))
                        countArray[path] = count
                    else:
                        countArray[path] = 1
                        # print(path + '=1')

    f.close()
    return countArray


# 入库
def insertIntoDB():
    print('start to connect to db\n')

    connection = pymysql.connect(**dbConfig)
    # todo

    return 0


getlog()


noParamTuple = getWithoutParam(LOG_PATH)
# f = open(RECORD_PATH,'a',encoding='utf-8')
f = open(RECORD_PATH, 'a')
f.write('----------------' + str(datetime.datetime.now()) + '------------------\n')
f.write('----------------pv------------------\n')
for requestPath in requestPathsWithoutParam:
    if requestPathsWithoutParam[requestPath] in noParamTuple[0]:
        requestCount = noParamTuple[0][requestPathsWithoutParam[requestPath]]
    else:
        requestCount = 0
    print('getWithoutParam ' + requestPath + ' = ' + str(requestCount))
    f.write('getWithoutParam ' + requestPath + ' = ' + str(requestCount) + '\n')

f.write('----------------uv------------------\n')
for requestPath in requestPathsWithoutParam:
    if requestPathsWithoutParam[requestPath] in noParamTuple[1]:
        requestCount = noParamTuple[1][requestPathsWithoutParam[requestPath]]
    else:
        requestCount = 0
    print('getWithoutParam ' + requestPath + ' = ' + str(requestCount))
    f.write('getWithoutParam ' + requestPath + ' = ' + str(requestCount) + '\n')

f.flush()
f.close()

countArray = getWithSource(LOG_PATH)
# f = open(RECORD_PATH,'a',encoding='utf-8')
f = open(RECORD_PATH, 'a')

for requestPath in requestPathsWithParam:
    if requestPathsWithParam[requestPath] in countArray:
        requestCount = countArray[requestPathsWithParam[requestPath]]
    else:
        requestCount = 0
    print('getWithParam ' + requestPath + ' = ' + str(requestCount))
    f.write('getWithParam ' + requestPath + ' = ' + str(requestCount) + '\n')
f.flush()
f.close()

# writeFile(requestPath)
# insertIntoDB()

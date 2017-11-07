import urllib.request
import re
import urllib
import tushare
import xlrd
import pymysql
import openpyxl
import time
import datetime
import csv
import os
from collections import deque

file_path = 'hjx_2.csv'


def get_page(url):
    req = urllib.request.Request(url, headers={
        'Connection': 'Keep-Alive',
        'Accept': 'text/html, application/xhtml+xml, */*',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'
    })
    opener = urllib.request.urlopen(req)
    page = opener.read()
    return page


def get_index_history_byNetease(share_code, query_time):
    code = share_code.split('.')[0]
    site = share_code.split('.')[1]
    if site == 'SH':
        code = '0' + code
    else:
        code = '1' + code
    price = None
    url = "http://quotes.money.163.com/service/chddata.html?code=%s&start=%s&end=%s&fields=TCLOSE;"
    for i in range(0, 15):
        if price is not None and float(price) != 0:
            return price
        query_time_temp = (query_time + datetime.timedelta(days=+i))
        target_url = url % (code, query_time_temp.strftime("%Y%m%d"), query_time_temp.strftime("%Y%m%d"))
        page = get_page(target_url).decode('gb2312')  # 该段获取原始数据
        page = page.split('\r\n')
        if page is not None and len(page) >= 2:
            if page[1] is not None and len(page[1].split(',')) >= 4:
                price = page[1].split(',')[3]

    return price


def read_file():
    data_map = {}
    error_map = {}
    index = 0
    with open(file_path, 'r') as fp:
        content = fp.readlines()
    for c in content:
        temp = c.split(',')
        share_code = temp[0]
        share_name = temp[1]
        date_time_now = datetime.datetime.strptime(temp[2].strip(), '%Y-%m-%d')
        date_time_before = (date_time_now + datetime.timedelta(days=-30))
        date_time_after = (date_time_now + datetime.timedelta(days=+30))

        try:
            price_now = get_index_history_byNetease(share_code, date_time_now)
            price_before = get_index_history_byNetease(share_code, date_time_before)
            price_after = get_index_history_byNetease(share_code, date_time_after)
            if price_now is None or price_before is None or price_after is None:
                print(str(share_code) + "," + share_name + "," + date_time_now.strftime('%Y-%m-%d') + ","
                      + "没有查询到数据")
            price_now = float(price_now)
            price_before = float(price_before)
            price_after = float(price_after)
            rate_of_return_first = (price_now - price_before) / price_before
            rate_of_return_second = (price_after - price_now) / price_now
            data_map[str(index)] = [str(share_code), share_name, date_time_now.strftime('%Y-%m-%d'), str(price_before),
                                    str(price_now), str(price_after), str(rate_of_return_first),
                                    str(rate_of_return_second)]
            index += 1
        except Exception as e:
            print(e)
            print(share_code + "," + share_name + "," + date_time_now.strftime('%Y-%m-%d') + "没有数据")
            error_map[str(len(error_map))] = share_code + "," + share_name + "," + date_time_now.strftime(
                '%Y-%m-%d') + "没有数据 " + str(e)
    if os.path.exists('result.csv'):
        os.remove('result.csv')
    print(data_map)
    with open('result.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for k, v in data_map.items():
            # print(v)
            spamwriter.writerow(v)

    if os.path.exists('error.csv'):
        os.remove('error.csv')
    print(error_map)
    with open('error.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        for k, v in error_map.items():
            # print(v)
            spamwriter.writerow(v)


def insert_mysql(code, name, notice_time, info_now, info_before, info_after):
    print("当前" + code)
    price_before = get_value(info_before)

    price_now = get_value(info_now)

    price_after = get_value(info_after)
    rate_of_return_first = (price_now - price_before) / price_before
    rate_of_return_second = (price_after - price_now) / price_now

    sql = "INSERT INTO shares_statistics (code,name,notice_time,price_before,price_now,price_after," \
          "rate_of_return_first,rate_of_return_second)VALUES('" + code + "','" + name + "','" + notice_time + \
          "','" + str(price_before) + "','" + str(price_now) + "','" + str(price_after) + "','" + \
          str(rate_of_return_first) + "','" + str(rate_of_return_second) + "')"
    print(sql)
    return sql


def get_value(temp):
    # print(temp)
    if temp is None:
        return 1.0
    if type(temp) == list:
        return temp[0].close.data.obj[0]
    return temp.close.data.obj[0]


# print(tushare.__version__)

# print(tushare.get_k_data('000063', '2017-07-07', '2017-07-07', index=True))

if __name__ == '__main__':
    # u = get_index_history_byNetease('000333.SZ', datetime.datetime.strptime('2015-03-29'.strip(), '%Y-%m-%d'))
    # list()
    # print(tushare.get_k_data('603017', '2017-06-08', '2017-06-08', index=True))
    read_file()

# data_list = read_xlsx()
# print(data_list)

# queue = deque()
# visited = set()
#
# url = 'http://news.dbanotes.net'
#
# queue.append(url)
# cnt = 0

# while queue:
#     url = queue.popleft()
#     visited |= {url}
#
#     print('已经获取：' + str(cnt) + '    正在抓取<---   ' + url)
#     cnt += 1
#     urlop=urllib.request.urlopen(url)
#     if 'html' not in urlop.getheader('Content-Type'):
#         continue
#
#     try:
#         data = urlop.read().decode('utf-8')
#     except:
#         continue
#
#     linkre=re.compile('href=\"(.+?)\"')
#     for x in linkre.findall(data):
#         if  'http' in x and x not in visited:
#             queue.append(x)
#             print('加入队列 --->  '+x)
# url = "http://www.baidu.com"
# data = urllib.request.urlopen(url).read()
# data = data.decode('UTF-8')
# print(data)

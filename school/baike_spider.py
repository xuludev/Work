"""
a web spider for baidu baike schools info in beijing
"""
import logging.handlers
import os
import urllib.parse

import pandas as pd
import pymysql.cursors
import requests
from bs4 import BeautifulSoup
from lxml import etree

LOG_FILE = 'baikespider.log'

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=5)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'

formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)

logger = logging.getLogger('baikespider')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

success_url_list = []
fail_url_list = []


class School():
    def __init__(self, name='', en_name='', nick_name='', xiaoxun='', introduction='', start_time='', level='',
                 location='', detailed_history='', facility=''):
        self.name = name
        self.en_name = en_name
        self.nick_name = nick_name
        self.xiaoxun = xiaoxun
        self.introduction = introduction
        self.start_time = start_time
        self.level = level
        self.location = location
        self.detailed_history = detailed_history
        self.facility = facility

    def __str__(self):
        return "{ " + str(self.name) + " ;" + self.en_name + " ;" + str(self.xiaoxun) + " ;" + str(
            self.introduction) + " ;" + self.start_time + " ;" + self.level + " ;" + self.location + " ;" \
               + self.detailed_history + " ;" + self.facility + " }"


def read_school_names(excel_path):
    df = pd.read_excel(excel_path, sheetname="Sheet1", index_col=False)['名称']

    return df.tolist()


def crawl(school_name):
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Host": "baike.baidu.com",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36"
    }
    request_url = "https://baike.baidu.com/item/%s" % urllib.parse.quote(school_name)
    response = requests.get(request_url, timeout=10, headers=headers)
    print('start crawling %s ' % school_name)
    if response.status_code == 200:
        school = School()
        try:
            html_raw = response.text.encode("Latin").decode("UTF-8")
            soup = BeautifulSoup(html_raw, "html5lib")
            name = soup.find_all("h1")[0].get_text()  # 学校名称
            introduction = soup.find_all(class_="para")[0].get_text().strip()  # 学校简介

            school.name = name
            school.introduction = introduction

            if len(soup.find_all(class_="basic-info cmn-clearfix")) > 0:
                en_name = soup.find_all(class_="basicInfo-item value")[1].get_text().strip()
                nick_name = soup.find_all(class_="basicInfo-item value")[2].get_text().strip()  # 学校简称
                xiaoxun = soup.find_all(class_="basicInfo-item value")[3].get_text().strip()  # 学校校训
                start_time = soup.find_all(class_="basicInfo-item value")[4].get_text().strip()  # 创办时间
                level = soup.find_all(class_="basicInfo-item value")[5].get_text().strip()  # 学校类型
                location = soup.find_all(class_="basicInfo-item value")[6].get_text().strip()  # 所属地址

                all_para_div = soup.find_all(class_="para")
                detailed_history = ""  # 学校历史
                for _ in range(2, len(all_para_div) - 1, 1):
                    detailed_history += all_para_div[_].get_text().strip()
                facility = all_para_div[-1].get_text()  # 学校设施

                school.en_name = en_name
                school.nick_name = nick_name
                school.xiaoxun = xiaoxun
                school.start_time = start_time
                school.level = level
                school.location = location
                school.detailed_history = detailed_history
                school.facility = facility
        except:
            pass
    else:
        print('ERROR')

    return school


def crawl_text(school_name):
    text_dir = 'D:/schools/'
    if not os.path.exists(text_dir):
        os.makedirs(text_dir)
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Host": "baike.baidu.com",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36"
    }
    request_url = "https://baike.baidu.com/item/%s" % urllib.parse.quote(school_name)
    response = requests.get(request_url, timeout=10, headers=headers)
    print('start crawling %s ' % school_name)
    if response.status_code == 200:
        html_raw = response.text.encode("Latin").decode("UTF-8")
        soup = BeautifulSoup(html_raw, "html5lib")
        if "您所访问的页面不存在..." not in soup.text:
            with open(os.path.join(text_dir, school_name + '.txt'), encoding='UTF-8', mode='wt') as f:
                f.write(soup.find_all(class_="main-content")[0].get_text())
                f.flush()
                f.close()
                logging.debug('%s has been written successfully~' % school_name)
    else:
        print("ERROR!")


def open_mysql():
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='19930620',
                                 db='hzau',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    return connection


def insert_to_mysql(school, connection):
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO school(name, en_name, nick_name, xiaoxun, introduction, start_time, level, location, detailed_history, facility)" \
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            cursor.execute(sql, (
                school.name, school.en_name, school.nick_name, school.xiaoxun, school.introduction, school.start_time,
                school.level, school.location, school.detailed_history, school.facility))
            connection.commit()

            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT * FROM school"
                cursor.execute(sql)
                result = cursor.fetchone()
                print(result)
    except:
        pass


def close_mysql(connection):
    connection.close()


if __name__ == '__main__':
    connection = open_mysql()
    schoolnames = read_school_names("D:/采集.xlsx")

    # for schoolname in schoolnames:
    #     crawl_text(schoolname)

    for schoolname in schoolnames:
        try:
            school = crawl(schoolname)
            if school is not None and school.name != "":
                insert_to_mysql(school, connection)
                logging.info(school.name)
        except:
            pass

    close_mysql(connection)

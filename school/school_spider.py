"""
a web spider for http://xuexiao.pinwaiyi.com/hy/
"""
import logging.handlers
import urllib.parse

import pandas as pd
import pymysql.cursors
import requests
from bs4 import BeautifulSoup

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
    def __init__(self, name='', href='', headmaster='', location='', region='', tel='', stage='', zipcode='',
                 pupil_num='', middler_num='', senior_num=''):
        self.name = name
        self.href = href
        self.headmaster = headmaster
        self.location = location
        self.region = region
        self.tel = tel
        self.stage = stage
        self.zipcode = zipcode
        self.pupil_num = pupil_num
        self.middler_num = middler_num
        self.senior_num = senior_num

    def __str__(self):
        return "{ " + str(self.name) + " ;" + self.href + " ;" + str(self.headmaster) + " ;" + str(
            self.location) + " ;" + self.region + " ;" + self.tel + " ;" + self.stage + " ;" \
               + self.zipcode + " ;" + self.pupil_num + " ;" + self.middler_num + " ;" + self.senior_num + " }"


def read_school_names(excel_path):
    df = pd.read_excel(excel_path, sheetname="Sheet1", index_col=False)['名称']

    return df.tolist()


def crawl(max_pn):
    headers = {
        "Host": "xuexiao.pinwaiyi.com",
        "Referer": "http://xuexiao.pinwaiyi.com/hy/list.php?fid=1&page=1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
    }
    request_url_list = ["http://xuexiao.pinwaiyi.com/hy/list.php?fid=1&page=%d" % (pn + 1) for pn in range(max_pn)]

    connection = open_mysql()
    for request_url in request_url_list:
        school = School()
        response = requests.get(request_url, timeout=10, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html5lib")
            for table in soup.find_all("table", class_="list"):
                try:
                    href = table.find_all(class_="td2")[0].a['href']  # 学校详情页面URL
                    name = table.find_all(class_="td2")[0].a.text.strip()  # 学校名称
                    headmaster_and_location = table.find_all(class_="td2")[0].find_all('div', class_="other2")[
                        0].get_text().strip()
                    headmaster = headmaster_and_location.split("              ")[0].strip()  # 校长
                    location = headmaster_and_location.split("              ")[1].strip()  # 学校地址
                    detail = crawl_school_detail(href)
                    region = detail[0]  # 地区
                    tel = detail[1]  # 联系方式
                    stage = detail[2]  # 学段
                    zipcode = detail[3]  # 邮编
                    headmaster = detail[4]  # 校长
                    pupil_num = detail[5]  # 小学生数量
                    middler_num = detail[6]  # 初中生数量
                    senior_num = detail[7]  # 高中生数量

                    school.name = name
                    school.href = href
                    school.headmaster = headmaster
                    school.location = location
                    school.region = region
                    school.tel = tel
                    school.stage = stage
                    school.zipcode = zipcode
                    school.pupil_num = pupil_num
                    school.middler_num = middler_num
                    school.senior_num = senior_num

                    if school is not None and school.name != "":
                        insert_to_mysql(school, connection)
                        print(str(school) + " has been inserted successfully~")
                except:
                    pass
        else:
            logging.error("ERROR " + request_url)

    close_mysql(connection)


def crawl_school_detail(school_href):
    detail = []
    headers = {
        "Host": "xuexiao.pinwaiyi.com",
        "Referer": "http://xuexiao.pinwaiyi.com/hy/list.php?fid=1&page=1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
    }
    response = requests.get(school_href, headers=headers, timeout=10)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html5lib")
        root_td = soup.find_all(class_="content")[-1]
        index = 0
        for each_td in root_td.find_all("tbody")[1].find_all("td"):
            if index % 2 == 1:
                detail.append(each_td.get_text().strip())
            index += 1
    else:
        logging.error(school_href)

    return detail


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
            sql = "INSERT INTO `school_info` (`name`, `href`, `headmaster`, `location`, `region`, `tel`, `stage`, " \
                  "`zipcode`, `pupil_num`, `middler_num`, `senior_num`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            cursor.execute(sql, (
                school.name, school.href, school.headmaster, school.location, school.region, school.tel,
                school.stage, school.zipcode, school.pupil_num, school.middler_num, school.senior_num))
            connection.commit()
    except:
        pass


def close_mysql(connection):
    connection.close()


if __name__ == '__main__':
    crawl(147)

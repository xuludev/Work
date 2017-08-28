"""
a web spider for baidu baike schools info in beijing powered by XPath
"""
import logging.handlers
import os
import urllib.parse

import pandas as pd
import pymysql.cursors
import requests
from bs4 import BeautifulSoup
from lxml import etree
from io import StringIO, BytesIO
from pymongo import MongoClient


def insert_item(item):
    """
    insert an object into mongodb
    :param item:
    :return:
    """
    client = MongoClient()
    db = client.baike.school
    result = db.insert_one(item)


def crawl_baike(school_name):
    """
    powered by XPath
    :return:
    """
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Host": "baike.baidu.com",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36"
    }
    request_url = "https://baike.baidu.com/item/%s" % urllib.parse.quote(school_name)
    response = requests.get(request_url, timeout=20, headers=headers)
    print('start crawling %s ...' % school_name)
    if response.status_code not in [403, 404]:
        try:
            school = {}
            html_raw = response.text.encode("Latin").decode("UTF-8")
            soup = BeautifulSoup(html_raw, "html5lib")

            parser = etree.HTMLParser()
            tree = etree.parse(StringIO(html_raw), parser)
            name = tree.xpath("//dd[@class='lemmaWgt-lemmaTitle-title']/h1/text()")[0]  # 学校名称
            introduction = ''.join(tree.xpath("//div[@class='lemma-summary']/div/text()"))  # 学校简介

            school['学校名称'] = name.strip()
            school['学校简介'] = introduction.strip()

            if len(soup.find_all(class_="basic-info cmn-clearfix")) > 0:
                for each_prop in soup.find_all(class_="basicInfo-item value"):
                    key = each_prop.find_previous().get_text().strip()
                    value = each_prop.get_text().strip()
                    school[key] = value

            for _ in soup.find_all('div', class_='para-title level-2'):
                title = _.h2.get_text().replace(name, '').strip()
                title_desc = _.find_next('div', class_="para").get_text().strip()
                school[title] = title_desc

            print(school)
        except:
            pass
    else:
        print('ERROR')

    return school


def read_school_names(excel_path):
    df = pd.read_excel(excel_path, sheetname="Sheet1", index_col=False)['名称']

    return df.tolist()


if __name__ == '__main__':
    schoolnames = read_school_names("D:/采集.xlsx")

    # print(crawl_baike('有一些啊大小'))
    for school_name in schoolnames:
        try:
            school = crawl_baike(school_name)
            if school is not None and school['学校名称'].strip() != "":
                insert_item(school)
        except:
            pass

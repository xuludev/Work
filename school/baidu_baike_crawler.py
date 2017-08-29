"""
a web spider for baidu baike schools info in beijing powered by XPath and BeautifulSoup
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


def crawl_baike_simple(school_name):
    """
    powered by XPath
    :return:
    @Deprecared
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
            html_raw = response.text.encode("Latin").decode("UTF-8").replace('\xa0', '').replace('\n', '')
            soup = BeautifulSoup(html_raw, "html5lib")

            parser = etree.HTMLParser()
            tree = etree.parse(StringIO(html_raw), parser)
            name = tree.xpath("//dd[@class='lemmaWgt-lemmaTitle-title']/h1/text()")[0]  # 学校名称
            introduction = ''.join(tree.xpath("//div[@class='lemma-summary']/div/text()"))  # 学校简介

            school['学校名称'] = name.strip()
            school['学校简介'] = introduction.strip()

            if len(soup.find_all(class_="basic-info cmn-clearfix")) > 0:
                for each_prop in soup.find_all(class_="basicInfo-item value", recursive=True, limit=3):
                    key = each_prop.find_previous().get_text().strip()
                    value = each_prop.get_text().strip()
                    school[key] = value

            for _ in soup.find_all('div', class_='para-title level-2', recursive=True, limit=3):
                title = _.h2.get_text().replace(name, '').strip()
                title_desc = _.find_next('div', class_="para").get_text().strip()
                school[title] = title_desc

            print(school)
        except:
            pass
    else:
        print('ERROR')

    return school


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
            html_raw = response.text.encode("Latin").decode("UTF-8").replace('\n', '')
            soup = BeautifulSoup(html_raw, "html5lib")

            parser = etree.HTMLParser()
            tree = etree.parse(StringIO(html_raw), parser)
            name = tree.xpath("//dd[@class='lemmaWgt-lemmaTitle-title']/h1/text()")[0]  # 学校名称
            introduction = ''.join(tree.xpath("//div[@class='lemma-summary']/div/text()"))  # 学校简介

            school['学校名称'] = name.strip()
            school['学校简介'] = introduction.strip()

            # 获取主要信息列表
            if len(soup.find_all(class_="basic-info cmn-clearfix")) > 0:
                for dl in soup.find_all(class_="basic-info cmn-clearfix")[0].find_all('dl', class_='basicInfo-block'):
                    for _ in range(len(dl.find_all('dt'))):
                        school[dl.find_all('dt')[_].get_text().strip().replace(' ', '').replace('\xa0', '')] = \
                            dl.find_all('dd')[_].get_text().strip()

            # 获取详细补充信息
            for _ in soup.find_all('div', class_='para-title level-2', recursive=True):
                title = _.h2.get_text().replace(name, '').strip()
                desc = ''
                for each_desc_div in _.find_next_siblings('div', class_="para"):
                    if each_desc_div['class'] == 'para':
                        break
                    desc += each_desc_div.get_text().strip()
                school[title.replace('\xa0', '')] = desc.replace('\xa0', '').replace(' ', '')

            # 参考资料
            reflist = []
            if len(soup.find_all(class_='reference-list')) > 0:
                for li in soup.find_all(class_='reference-list')[0]:
                    if len(li.find_all('a')) > 1:
                        ref = {}
                        ref_name = li.find_all('a')[1].text.strip()
                        ref_url = li.find_all('a')[1]['href'].strip()
                        ref_source = "".join([_.get_text().strip() for _ in li.find_all('span')])
                        ref['参考名称'] = ref_name
                        ref['参考链接'] = ref_url
                        ref['参考来源'] = ref_source

                        reflist.append(ref)

            school['参考资料'] = reflist

            # 词条统计
            try:
                word_stat = {}
                if len(soup.find_all('dd', class_="description")) > 0:
                    li_list = soup.find_all('dd', class_="description", recursive=True)[0].find_all('li')
                    word_stat[li_list[0].text.split('：')[0].strip()] = li_list[0].span.text
                    for i in range(1, len(li_list), 1):
                        word_stat[li_list[i].text.split('：')[0].strip()] = li_list[i].text.split('：')[1].strip()
                school['词条统计'] = word_stat
            except:
                pass

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

    # school = crawl_baike("中国人民大学附属中学")
    for school_name in schoolnames:
        try:
            school = crawl_baike(school_name)
            if school is not None and school['学校名称'].strip() != "":
                insert_item(school)
        except:
            pass

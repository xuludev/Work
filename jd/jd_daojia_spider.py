import json
import time

import pandas as pd
import requests


def get_all_categories(storeId):
    url = 'https://daojia.jd.com/client'
    headers = {
        'Accept': 'application/json',
        'Host': 'daojia.jd.com',
        'Referer': 'https://daojia.jd.com/html/index.html?channel=BDPZH&sem=biaoti',
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1',
    }
    cookies = dict(
        cookies_are='TrackID=1ruoo4msJ5XnJQ-ZEPbDoH5tHWZvkOvXHh04vryMySz80Alog9keg32PqHlucdoq2Ip42j7zGkH70Mret'
                    'LQTMv4OBpygbHs294STcONVvTl4; pinId=FEhkcrf860arB7TdWYL4jg; __jdv=122270672|direct|-|none|-'
                    '|1494839453209; ipLoc-djd=1-72-2799-0; ipLocation=%u5317%u4EAC; __jda=122270672.14835072166'
                    '751503477682.1483507217.1494839453.1495080995.13; __jdu=14835072166751503477682; 3AB9D23F7A4'
                    'B3C9B=3L37OQGBAWKZRPD6IIXAP2ZSGQWU4U7B2KOD5NDX2JZWCYGXMK7XDLRUFGEXUHKAMJEIA66NO7KWHIMJ52HC565HHM; '
                    'deviceid_pdj_jd=H5_DEV_129F8238-D008-4A7C-B4B5-B57953FF1C87; cart_uuid=c5ab613aab931a59; h5abtest=%5'
                    'B%7B%22experimentName%22%3A%22search_andor_test%22%2C%22testTag%22%3A%22search_andor_test%3Aor%3A2017-0'
                    '5-27%2010%3A41%3A08%22%2C%22duration%22%3A10800000%7D%2C%7B%22duration%22%3A%2210800000%22%2C%22experiment'
                    'Name%22%3A%22glbStorePage%22%2C%22testTag%22%3A%22glbStorePage%3Ashow%3A2017-05-27%2011%3A22%3A21%22%7D%5D')
    payload = {
        'functionId': 'store/storeDetailV220',
        'appVersion': '4.2.0',
        'appName': 'paidaojia',
        'platCode': 'H5'
    }

    body = {"storeId": str(storeId), "skuId": "", "activityId": "", "promotionType": "", "longitude": 0,
            "latitude": 0}
    payload['body'] = json.dumps(body, ensure_ascii=False)

    categories = []
    titles = []
    response = requests.get(url, headers=headers, cookies=cookies, params=payload)
    if response.status_code == 200:
        cate_list = response.json()['result']['cateList']
        for cate in cate_list:
            if len(cate['childCategoryList']) > 0:
                for _ in cate['childCategoryList']:
                    if _['catId'] != '':
                        categories.append(_['catId'])
                        titles.append(_['title'])
            else:
                categories.append(cate['catId'])
                titles.append(cate['title'])
    else:
        print('Sorry, an error occurred!')

    return categories, titles


def get_commodity(storeId, catId):
    result = []
    url_ = 'https://daojia.jd.com/client'
    payload = {
        'functionId': 'productsearch/search',
        'appVersion': '4.2.0',
        'appName': 'paidaojia',
        'platCode': 'H5',
    }

    body = {"key": "", "catId": str(catId), "storeId": str(storeId), "sortType": 1, "page": 1, "pageSize": 1000,
            "cartUuid": "", "promotLable": "", "timeTag": str(int(time.time()))}

    payload['body'] = json.dumps(body, ensure_ascii=False)

    headers = {
        'Accept': 'application/json',
        'Host': 'daojia.jd.com',
        'Referer': 'https://daojia.jd.com/html/index.html?channel=BDPZH&sem=biaoti',
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1',
    }

    cookies = dict(
        cookies_are='TrackID=1ruoo4msJ5XnJQ-ZEPbDoH5tHWZvkOvXHh04vryMySz80Alog9keg32PqHlucdoq2Ip42j7zGkH70Mret'
                    'LQTMv4OBpygbHs294STcONVvTl4; pinId=FEhkcrf860arB7TdWYL4jg; __jdv=122270672|direct|-|none|-'
                    '|1494839453209; ipLoc-djd=1-72-2799-0; ipLocation=%u5317%u4EAC; __jda=122270672.14835072166'
                    '751503477682.1483507217.1494839453.1495080995.13; __jdu=14835072166751503477682; 3AB9D23F7A4'
                    'B3C9B=3L37OQGBAWKZRPD6IIXAP2ZSGQWU4U7B2KOD5NDX2JZWCYGXMK7XDLRUFGEXUHKAMJEIA66NO7KWHIMJ52HC565HHM; '
                    'deviceid_pdj_jd=H5_DEV_129F8238-D008-4A7C-B4B5-B57953FF1C87; cart_uuid=c5ab613aab931a59; h5abtest=%5'
                    'B%7B%22experimentName%22%3A%22search_andor_test%22%2C%22testTag%22%3A%22search_andor_test%3Aor%3A2017-0'
                    '5-27%2010%3A41%3A08%22%2C%22duration%22%3A10800000%7D%2C%7B%22duration%22%3A%2210800000%22%2C%22experiment'
                    'Name%22%3A%22glbStorePage%22%2C%22testTag%22%3A%22glbStorePage%3Ashow%3A2017-05-27%2011%3A22%3A21%22%7D%5D')

    response = requests.get(url_, headers=headers, cookies=cookies, params=payload)
    if response.status_code == 200:
        commodity_list = response.json()['result']['searchResultVOList']
        for commodity in commodity_list:
            try:
                # print(commodity)
                skuName = commodity['skuName']
                realTimePrice = commodity['realTimePrice']  # price after discount
                basicPrice = commodity['basicPrice'] if commodity[
                                                            'basicPrice'] != '' else realTimePrice  # price before discount
                stockCount = commodity['stockCount']
                result.append([skuName, realTimePrice, basicPrice, stockCount])
            except:
                pass

    return result


if __name__ == '__main__':
    catregories, titles = get_all_categories(11002322)
    print(catregories)

    for i in range(len(catregories)):
        commodity_list = get_commodity(11002322, catregories[i])
        print(commodity_list)
        col = [
            u'商品名称',
            u'折后价',
            u'原价',
            u'库存量']
        df = pd.DataFrame(commodity_list, columns=col)
        df.to_excel('./京西菜市-锦华店/%s.xlsx' % titles[i], '商品')
        print('Processing %s done!' % titles[i])

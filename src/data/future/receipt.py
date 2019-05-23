# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime
import re

from src.data.future.setting import RECEIPT_DIR
from src.data.future.utils import get_download_file_index
from log import LogHandler
from src.util import get_html_text

log = LogHandler('data.log')

HEADER = ['日期', '品种', '期货仓单', '仓单变化']

DATA_BEGIN_DATE = {'czce': datetime(2005, 4, 29),
                   'shfe': datetime(2008, 10, 6),
                   'dce': datetime(2000, 5, 8)}


def download_shfe_receipt_by_date(date: datetime):
    """
    抓取上海商品交易所注册仓单数据,
    20140519(包括)至今
    http://www.shfe.com.cn/data/dailydata/20190520dailystock.dat
    20081006至20140516(包括)
    http://www.shfe.com.cn/data/dailydata/20140516dailystock.html
    http://www.shfe.com.cn/txt.jsp
    20100126、20101029日期 英文版本
    20100416
    20130821日期交易所数据丢失
    :param date: datetime
    :return: str
    """
    assert date <= datetime.today()

    if date > datetime(2014, 5, 18):
        url_template = "http://www.shfe.com.cn/data/dailydata/{}dailystock.dat"
        url = url_template.format(date.strftime('%Y%m%d'))
        text_data = get_html_text(url)
        json_data = json.loads(text_data)
        data = pd.DataFrame(json_data['o_cursor'])

    elif date > datetime(2008, 1, 5):
        url_template = "http://www.shfe.com.cn/data/dailydata/{}dailystock.html"
        url = url_template.format(date.strftime('%Y%m%d'))
        data = pd.read_html(url, encoding='unicode')[0]
    else:
        data = pd.DataFrame()

    return data


def download_dce_receipt_by_date(date: datetime):
    """
    抓取大连商品交易所注册仓单数据,
    数据从20060106开始，每周五更新仓单数据。直到20090407起，每交易日都更新仓单数据
    http://www.dce.com.cn/publicweb/quotesdata/wbillWeeklyQuotes.html?wbillWeeklyQuotes.variety=all&year=2019&month=4&day=20

    :param date: datetime
    :return: str
    """
    assert date <= datetime.today()

    if date > datetime(2009, 4, 6) or (date > datetime(2009, 1, 5) and date.weekday() == 4):
        url_template = \
            "http://www.dce.com.cn/publicweb/quotesdata/wbillWeeklyQuotes.html?wbillWeeklyQuotes.variety=all&year={}&month={}&day={}"
    else:
        return pd.DataFrame()

    url = url_template.format(date.year, date.month - 1, date.day)
    data = pd.read_html(url, encoding='unicode')[0]

    return data


def download_czce_receipt_by_date(date: datetime):
    """
    抓取郑州商品交易所注册仓单数据,
    20080215，20080222，20080229  是周报
    20100303(包括)至20100824(包括)
    'http://www.czce.com.cn/cn/exchange/jyxx/sheet/sheet20100824.html'
    20100825(包括)至20150930(包括)
    'http://www.czce.com.cn/cn/exchange/%s/datawhsheet/20150930.htm'
    http://www.czce.com.cn/cn/exchange/2015/datawhsheet/20150930.txt
    http://www.czce.com.cn/cn/exchange/2015/datawhsheet/20150930.xls
    20151008(包括)至今
    'http://www.czce.com.cn/cn/DFSStaticFiles/Future/2015/20151008/FutureDataWhsheet.htm'
    'http://www.czce.com.cn/cn/DFSStaticFiles/Future/2015/20151112/FutureDataWhsheet.txt'
    'http://www.czce.com.cn/cn/DFSStaticFiles/Future/2015/20151008/FutureDataWhsheet.xls'
    :return: str
    """
    assert date <= datetime.today()

    if date > datetime(2015, 10, 7):
        url_template = 'http://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/{}/FutureDataWhsheet.htm'
        url = url_template.format(date.year, date.strftime('%Y%m%d'))
    elif date > datetime(2010, 8, 24):
        url_template = 'http://www.czce.com.cn/cn/exchange/{}/datawhsheet/{}.htm'
        url = url_template.format(date.year, date.strftime('%Y%m%d'))
    elif date > datetime(2010, 3, 3) or (date > datetime(2010, 2, 14) and date.weekday() == 4):
        url_template = 'http://www.czce.com.cn/cn/exchange/jyxx/sheet/sheet{}.html'
        url = url_template.format(date.strftime('%Y%m%d'))
    else:
        return pd.DataFrame()

    text_data = get_html_text(url)
    data = pd.read_html(text_data, encoding='gb2312')

    return data


def download_receipt_by_date(date, file_path, market='dce'):
    """
    从交易所网站获取某天的所有行情数据，存盘并返回pd.DataFrame
    :param date: 需要数据的日期
    :param file_path: 存储文件的地址
    :param market: 交易所代码
    :return: pd.DataFrame
    """
    assert market in ['dce', 'czce', 'shfe']

    get_exchange_hq_func = {'czce': download_czce_receipt_by_date,
                            'shfe': download_shfe_receipt_by_date,
                            'dce': download_dce_receipt_by_date}

    data = get_exchange_hq_func[market](date)
    date_str = date.strftime('%Y%m%d')

    if is_data_empty(data):
        log.warning('{} {} data:{} is not downloaded! '.format(market, date_str, category))
        # time.sleep(np.random.rand() * TIME_WAITING * 3)
        return False

    if isinstance(data, pd.DataFrame):
        data.to_csv(file_path, encoding='gb2312')
    else:
        file_path.write_text(data)

    return True


def download_receipt_by_dates(market, start):
    """
    根据日期连续下载交易所日仓单数据
    :param start:
    :param market:
    :return True False: 说明不用下载数据

    """
    assert market in ['dce', 'czce', 'shfe']

    target = RECEIPT_DIR / market

    file_index = get_download_file_index(target, start=start)

    if file_index.empty:
        return False

    for dt in file_index:
        print('{} downloading {} {} receipt data!'.format(
            datetime.now().strftime('%H:%M:%S'), market, dt.strftime('%Y-%m-%d')))
        date_str = dt.strftime('%Y%m%d')
        file_path = target / '{}.day'.format(date_str)
        download_receipt_by_date(dt, file_path, market)
        # time.sleep(np.random.rand() * TIME_WAITING)
    return True


if __name__ == '__main__':
    print(datetime.now())
    start = datetime(2019, 5, 16)
    download_shfe_receipt_by_date(start)
    # 数据从20060106开始，每周五更新仓单数据。直到20090407起，每交易日都更新仓单数据
    # download_dce_receipt_by_date(start)
    print(datetime.now())

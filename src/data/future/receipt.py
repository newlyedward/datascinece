# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import json
import time
import re
from datetime import datetime, timedelta

from pymongo import DESCENDING

from log import LogHandler

from src.data import conn
from src.data.future.setting import RECEIPT_DIR, NAME2CODE_MAP
from src.data.future.utils import get_download_file_index, get_exist_files, split_symbol
from src.util import get_html_text

log = LogHandler('data.log')

DATA_BEGIN_DATE = {'czce': datetime(2008, 2, 15),
                   'shfe': datetime(2008, 10, 6),
                   'dce': datetime(2006, 1, 6)}

TIME_WAITING = 1


def download_shfe_receipt_by_date(date: datetime):
    """
    抓取上海商品交易所注册仓单数据,
    20140519(包括)至今
    http://www.shfe.com.cn/data/dailydata/20190520dailystock.dat
    20081006至20140516(包括)
    http://www.shfe.com.cn/data/dailydata/20140516dailystock.html
    http://www.shfe.com.cn/txt.jsp
    20100126、20101029日期 英文版本
    20100416 格式不一样
    20130821日期交易所数据丢失
    :param date: datetime
    :return: str
    """
    assert date <= datetime.today()

    if date > datetime(2014, 5, 18):
        url_template = "http://www.shfe.com.cn/data/dailydata/{}dailystock.dat"
        url = url_template.format(date.strftime('%Y%m%d'))
        try:
            text_data = get_html_text(url)
            json_data = json.loads(text_data)
            data = pd.DataFrame(json_data['o_cursor'])
        except:
            log.warning("{} shfe receipt data is not exist!".format(date.strftime('%Y%m%d')))
            data = pd.DataFrame()

    elif date > datetime(2008, 1, 5):
        url_template = "http://www.shfe.com.cn/data/dailydata/{}dailystock.html"
        url = url_template.format(date.strftime('%Y%m%d'))
        try:
            data = pd.read_html(url, encoding='unicode')[0]
        except ValueError:
            log.warning("{} shfe receipt data is not exist!".format(date.strftime('%Y%m%d')))
            data = pd.DataFrame()
    else:
        data = pd.DataFrame()
        log.info("Shfe has no {} receipt data!".format(date.strftime('%Y%m%d')))

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

    if date > datetime(2009, 4, 6) or (date > datetime(2006, 1, 5) and date.weekday() == 4):
        url_template = \
            "http://www.dce.com.cn/publicweb/quotesdata/wbillWeeklyQuotes.html?" \
            "wbillWeeklyQuotes.variety=all&year={}&month={}&day={}"
    else:
        log.info("Dce has no {} receipt data!".format(date.strftime('%Y%m%d')))
        return pd.DataFrame()

    try:
        url = url_template.format(date.year, date.month - 1, date.day)
        data = pd.read_html(url, encoding='unicode')[0]
    except ValueError:
        log.warning("{} dce receipt data is not exist!".format(date.strftime('%Y%m%d')))
        data = pd.DataFrame()

    return data


def download_czce_receipt_by_date(date: datetime):
    """
    抓取郑州商品交易所注册仓单数据,
    20080215，20080222，20080229  是周报
    20080303(包括)至20100824(包括)  20090820数据不存在
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
        index = 1
    elif date > datetime(2010, 8, 24):
        # 仓单数据从第4个table开始，没有合约乘数
        url_template = 'http://www.czce.com.cn/cn/exchange/{}/datawhsheet/{}.htm'
        url = url_template.format(date.year, date.strftime('%Y%m%d'))
        index = 3
    elif date > datetime(2008, 3, 2) or (date > datetime(2008, 2, 14) and date.weekday() == 4):
        # 只有一张表
        url_template = 'http://www.czce.com.cn/cn/exchange/jyxx/sheet/sheet{}.html'
        url = url_template.format(date.strftime('%Y%m%d'))
        index = 1
    else:
        log.info("Czce has no {} receipt data!".format(date.strftime('%Y%m%d')))
        return pd.DataFrame()

    try:
        text_data = get_html_text(url)
        df = pd.read_html(text_data, encoding='gb2312')
        data = pd.concat(df[index:])
    except:
        log.warning("{} czce receipt data is not exist!".format(date.strftime('%Y%m%d')))
        data = pd.DataFrame()

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

    if data.empty:
        # log.warning('{} {} data is not downloaded! '.format(market, date_str))
        # time.sleep(np.random.rand() * TIME_WAITING * 3)
        return False

    assert isinstance(data, pd.DataFrame)
    data.to_csv(file_path, encoding='gb2312')

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
        file_path = target / '{}.csv'.format(date_str)
        download_receipt_by_date(dt, file_path, market)
        time.sleep(np.random.rand() * TIME_WAITING)
    return True


def transfer_shfe_receipt(date, file_path):
    """
    抓取上海商品交易所注册仓单数据,
    20140519(包括)至今  json数据格式转换给dataframe
    20081006至20140516(包括)
    20100126、20101029日期 英文版本
    20100416 格式不一样
    :param file_path:
    :param date: datetime
    :return: str
    """
    data = pd.DataFrame(columns=['code', 'datetime', 'receipt', 'market'])

    receipt_df = pd.read_csv(file_path, encoding='gb2312', header=0, index_col=False)

    if receipt_df.empty:
        log.warning('Shfe {} receipt file is empty!')
        return data

    if date > datetime(2014, 5, 18):
        split_re, index = split_symbol(r'(\S+)\$\$\S+$', receipt_df['VARNAME'])
        data['code'] = split_re.transform(
            lambda x: NAME2CODE_MAP['exchange'][x[1]] if x[1] in NAME2CODE_MAP['exchange'] else x[1])
        data['code'].duplicated()
        split_re, index = split_symbol(r'总计\$\$Total', receipt_df['WHABBRNAME'])
        data['receipt'] = receipt_df.loc[index, 'WRTWGHTS']
        data['market'] = 'shfe'
        data['datetime'] = date

    elif date > datetime(2008, 1, 5):


    else:
        log.error("{} shfe receipt date is not exist!".format(date.strftime('%Y%m%d')))

    return data


def transfer_dce_receipt(date, file_path):
    """

    :param date:
    :param file_path:
    :return:
    """
    data = pd.DataFrame(columns=['code', 'datetime', 'receipt', 'market'])

    receipt_df = pd.read_csv(file_path, encoding='gb2312', header=0, index_col=False)

    if receipt_df.empty:
        log.warning('Dce {} receipt file is empty!')
        return data

    split_re, index = split_symbol(r'(\S+)小计$', receipt_df['品种'])
    data['code'] = split_re.transform(
        lambda x: NAME2CODE_MAP['exchange'][x[1]] if x[1] in NAME2CODE_MAP['exchange'] else x[1])
    data['receipt'] = receipt_df.loc[index, '今日仓单量']
    data['market'] = 'dce'
    data['datetime'] = date


def insert_receipt_to_mongo():
    """
    下载数据文件，插入mongo数据库
    :return:
    """

    # market = ['dce', 'czce', 'shfe']
    market = ['shfe', 'czce', 'dce']
    cursor = conn['receipt']

    # transfer_exchange_hq_func = {
    #     'cffex': transfer_cffex_future_hq,
    #     'czce': transfer_czce_future_hq,
    #     'shfe': transfer_shfe_future_hq,
    #     'dce': transfer_dce_future_hq
    # }

    for m in market:
        # 下载更新行情的原始数据
        filer_dict = {"market": m}
        projection = {"_id": 0, "datetime": 1}

        start = cursor.find_one(filer_dict, projection=projection, sort=[("datetime", DESCENDING)])
        if start is None:
            start = DATA_BEGIN_DATE[m]
        else:
            start = start['datetime'] + timedelta(1)
        download_receipt_by_dates(m, start)

        # 需要导入数据库的原始数据文件
        # file_df = get_insert_mongo_files(m, c, start=start)
        # file_df = get_exist_files(RECEIPT_DIR / m)
        # file_df = file_df[start:]
        #
        # if file_df.empty:
        #     print('{} {} hq is updated before!'.format(m, t))
        #     continue
        # columns_map = COLUMNS_MAP[t][m].copy()
        # for row in file_df.itertuples():
        #     df = transfer_exchange_hq_func[c][m](row.Index, row.filepath, columns_map)
        #     if df.empty:
        #         log.error("Transform {} {} {}data failure, please check program.".format(m, t, row.Index))
        #         continue
        #     result = cursor.insert_many(df.to_dict('records'))
        #     if result:
        #         print('{} {} {} insert success.'.format(m, t, row.filepath.name))
        #         move_data_files(row.filepath)
        #     else:
        #         print('{} {} {} insert failure.'.format(m, t, row.filepath.name))
        # print('{} {} hq is updated now!'.format(m, t))


if __name__ == '__main__':
    print(datetime.now())
    # start_date = datetime(2019, 5, 16)
    # download_receipt_by_date(start_date)
    # 数据从20060106开始，每周五更新仓单数据。直到20090407起，每交易日都更新仓单数据
    # download_dce_receipt_by_date(start_date)
    insert_receipt_to_mongo()
    print(datetime.now())

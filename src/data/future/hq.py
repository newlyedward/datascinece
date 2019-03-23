# -*- coding: utf-8 -*-
import json
import re
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.data.future.setting import RAW_HQ_DIR, PROCESSED_HQ_DIR, HQ_COLUMNS_PATH, CODE2NAME_TABLE
from src.data.future.utils import get_download_file_index
from src.data.setting import DATE_PATTERN
from src.data.util import get_post_text, get_html_text, connect_mongo, read_mongo, to_mongo
from src.log import LogHandler

TIME_WAITING = 60
log = LogHandler('future.hq.log')


def is_data_empty(date_str, text):
    if not isinstance(text, str):
        log.info('{} is fail, status: {}'.format(date_str, text))
        return True
    elif len(text) < 110:
        log.info('{} is null, status: {}'.format(date_str, text))
        return True
    else:
        return False


def get_cffex_hq_by_date(date: datetime, category=0):
    """
    获取中国金融期货交易所交易所日交易数据 datetime(2010, 4, 30)
    http://www.cffex.com.cn/sj/hqsj/rtj/201903/13/20190313_1.csv
    没有期权，预留接口

    :param date: datetime
    :param category: 行情类型, 0期货 或 1期权
    :return 不成功返回0

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    # url_template = 'http://www.cffex.com.cn/sj/hqsj/rtj/{}/{}/{}_1.csv'
    url_template = 'http://www.cffex.com.cn/fzjy/mrhq/{}/{}/{}_1.csv'
    url = url_template.format(date.strftime('%Y%m'), date.strftime('%d'), date.strftime('%Y%m%d'))
    # url_template = 'http://www.cffex.com.cn/sj/hqsj/rtj/{}/{}/index.xml'
    # url = url_template.format(date.strftime('%Y%m'), date.strftime('%d'))

    return get_html_text(url)


def get_cffex_hq_by_dates(start=None, end=None, category=0):
    """
    根据日期连续下载中国金融期货交易所日交易数据 20100430 期货数据起始日
    :param start: datetime
    :param end: datetime
    :param start:
    :param category: 行情类型, 0期货 或 1期权
    :return True or False
    """
    assert category in [0, 1]

    start = datetime(2010, 4, 30) if category == 0 else datetime.today() \
        if start is None else start
    end = datetime.today() if end is None else end

    target = RAW_HQ_DIR[category] / 'cffex'

    file_index = get_download_file_index(target, start=start, end=end)

    if file_index.empty:
        return False

    for date in file_index:
        print(date)
        text = get_cffex_hq_by_date(date, category=category)
        date_str = date.strftime('%Y%m%d')
        file_path = target / '{}_daily.txt'.format(date_str)

        assert isinstance(text, str)
        if not text.startswith('合约代码'):
            log.warning('Cffex {} data is not downloaded! '.format(date_str))
            time.sleep(np.random.rand() * TIME_WAITING * 3)
            continue

        file_path.write_text(text)
        time.sleep(np.random.rand() * TIME_WAITING)
    return True


def get_czce_hq_by_date(date: datetime, category=0):
    """
    获取郑州商品交易所日交易数据
    http://www.czce.com.cn/cn/DFSStaticFiles/Future/2019/20190314/FutureDataDaily.txt
    http://www.czce.com.cn/cn/DFSStaticFiles/Future/2019/20190314/FutureDataDaily.htm
    期权 datetime(2017, 4, 19)
    http://www.czce.com.cn/cn/DFSStaticFiles/Option/2018/20180816/OptionDataDaily.htm
    http://www.czce.com.cn/cn/DFSStaticFiles/Option/2017/20171109/OptionDataDaily.htm
    datetime(2015, 10, 8)
    http://www.czce.com.cn/cn/exchange/2015/datadaily/20150821.htm
    http://www.czce.com.cn/cn/exchange/2015/datadaily/20150930.txt
    datetime(2010, 8, 24)
    http://www.czce.com.cn/cn/exchange/jyxx/hq/hq20100806.html
    datetime(2005, 4, 29)

    :param date: datetime
    :param category: 行情类型, 0期货 或 1期权
    :return 不成功返回0

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    index = 0

    if date < datetime(2005, 4, 29):
        return pd.DataFrame()
    elif date < datetime(2010, 8, 24):
        url_template = 'http://www.czce.com.cn/cn/exchange/jyxx/hq/hq{}.html'
        url = url_template.format(date.strftime('%Y%m%d'))
        index = 1
    elif date < datetime(2015, 10, 8):
        url_template = 'http://www.czce.com.cn/cn/exchange/{}/datadaily/{}.htm'
        url = url_template.format(date.year, date.strftime('%Y%m%d'))
        index = 3
    else:
        template = ['http://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/{}/FutureDataDaily.htm',
                    'http://www.czce.com.cn/cn/DFSStaticFiles/Option/{}/{}/OptionDataDaily.htm']
        url_template = template[category]
        url = url_template.format(date.year, date.strftime('%Y%m%d'))

    text = get_html_text(url)

    if is_data_empty(date.strftime('%Y%m%d'), text):
        return pd.DataFrame()

    return pd.read_html(text, header=0)[index]


def get_czce_hq_by_dates(start=None, end=None, category=0):
    """
    根据日期连续下载郑州商品交易所日交易数据 20050429 期货数据起始日 20170419 期权数据起始日
    :param start: datetime
    :param end: datetime
    :param start:
    :param category: 行情类型, 0期货 或 1期权
    :return pd.DataFrame
    """
    assert category in [0, 1]

    start = datetime(2005, 4, 29) if category == 0 else datetime(2017, 4, 19) \
        if start is None else start
    end = datetime.today() if end is None else end

    target = RAW_HQ_DIR[category] / 'czce'

    file_index = get_download_file_index(target, start=start, end=end)

    if file_index.empty:
        return False

    for date in file_index:
        print(date)
        df = get_czce_hq_by_date(date, category=category)
        date_str = date.strftime('%Y%m%d')
        file_path = target / '{}_daily.txt'.format(date_str)
        if df.empty:
            log.warning('Czce {} data is not downloaded! '.format(date_str))
            time.sleep(np.random.rand() * TIME_WAITING * 3)
            continue
        df.to_csv(file_path, encoding='gb2312')
        time.sleep(np.random.rand() * TIME_WAITING)

    return True


def get_shfe_hq_by_date(date: datetime, category=0):
    """
    获取上海商品交易所日交易数据 20020108/20090105 期货数据起始日（还可以往前取） 2018921 期权数据起始日
    http://www.shfe.com.cn/data/dailydata/kx/kx20190318.dat
    http://www.shfe.com.cn/data/dailydata/option/kx/kx20190315.dat
    :param date: datetime
    :param category: 行情类型, 0期货 或 1期权
    :return 不成功返回0

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    url_template = ['http://www.shfe.com.cn/data/dailydata/kx/kx{}.dat',
                    'http://www.shfe.com.cn/data/dailydata/option/kx/kx{}.dat']
    url = url_template[category].format(date.strftime('%Y%m%d'))

    return get_html_text(url)


def get_shfe_hq_by_dates(start=None, end=None, category=0):
    """
    根据日期连续下载上海商品交易所日交易数据 20020108/20090105 期货数据起始日 2018921 期权数据起始日
    20040625 期货数据缺失
    :param start: datetime
    :param end: datetime
    :param start:
    :param category: 行情类型, 0期货 或 1期权
    :return pd.DataFrame
    """
    assert category in [0, 1]

    start = datetime(2002, 1, 8) if category == 0 else datetime(2018, 9, 21) \
        if start is None else start
    end = datetime.today() if end is None else end

    target = RAW_HQ_DIR[category] / 'shfe'

    file_index = get_download_file_index(target, start=start, end=end)

    if file_index.empty:
        return False

    for date in file_index:
        print(date)
        text = get_shfe_hq_by_date(date, category=category)
        date_str = date.strftime('%Y%m%d')
        file_path = target / '{}_daily.txt'.format(date_str)

        if is_data_empty(date_str, text):
            log.warning('Shfe {} data is not downloaded! '.format(date_str))
            time.sleep(np.random.rand() * TIME_WAITING * 3)
            continue

        file_path.write_text(text)
        time.sleep(np.random.rand() * TIME_WAITING)

    return True


def get_dce_hq_by_date(date: datetime, code='all', category=0):
    """
    获取大连商品交易所日交易数据 20050104 期货数据起始日 2017331 期权数据起始日
    url = 'http://www.dce.com.cn//publicweb/quotesdata/dayQuotesCh.html'
    url = 'http://www.dce.com.cn/publicweb/quotesdata/exportDayQuotesChData.html'

    form.data =
        dayQuotes.variety	    : all/y/m
        dayQuotes.trade_type	: 0/1  0:期货 1:期权
        year	    : 2019
        month	    : 2 实际月份-1
        day	        : 14
        exportFlag  : txt excel:xls
    :param code: 商品代码
    :param date: datetime
    :param category: 行情类型, 0期货 或 1期权
    :return 不成功返回0

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    url = 'http://www.dce.com.cn/publicweb/quotesdata/exportDayQuotesChData.html'
    form_data = {'dayQuotes.variety': code,
                 'dayQuotes.trade_type': category,
                 'year': date.year,
                 'month': date.month - 1,
                 'day': date.day,
                 'exportFlag': 'txt'}
    return get_post_text(url, form_data)


def get_dce_hq_by_dates(start=None, end=None, category=0):
    """
    根据日期连续下载大连商品交易所日交易数据 20050104 期货数据起始日 2017331 期权数据起始日
    20000508 开始数据，商品名称 大豆
    :param start: datetime
    :param end: datetime
    :param start:
    :param category: 行情类型, 0期货 或 1期权
    :return True False: 说明不用下载数据

    """
    assert category in [0, 1]

    start = datetime(2000, 5, 8) if category == 0 else datetime(2017, 3, 31) \
        if start is None else start
    end = datetime.today() if end is None else end

    target = RAW_HQ_DIR[category] / 'dce'

    file_index = get_download_file_index(target, start=start, end=end)

    if file_index.empty:
        return False

    for date in file_index:
        print(date)
        text = get_dce_hq_by_date(date, category=category)
        date_str = date.strftime('%Y%m%d')
        file_path = target / '{}_daily.txt'.format(date_str)

        if is_data_empty(date_str, text):
            log.warning('Czce {} data is not downloaded! '.format(date_str))
            time.sleep(np.random.rand() * TIME_WAITING * 3)
            continue

        file_path.write_text(text)
        time.sleep(np.random.rand() * TIME_WAITING)

    return True


def reload_hq_by_date(date, filepath, market='dce', category=0):
    """
    从交易所网站获取某天的所有行情数据，存盘并返回pd.DataFrame
    :param date: 需要数据的日期
    :param filepath: 存储文件的地址
    :param market: 交易所代码
    :param category: 0:期货 1：期权
    :return: pd.DataFrame
    """
    assert category in [0, 1]
    if market == 'dce':
        text = get_dce_hq_by_date(date, category=category)
        filepath.write_text(text)
        return pd.read_csv(filepath, encoding='gb2312', header=0, index_col=False,
                           sep='\s+', thousands=',').dropna()
    elif market == 'shfe':
        text = get_shfe_hq_by_date(date, category=category)
        filepath.write_text(text)
        return pd.DataFrame(json.loads(filepath.read_text())['o_curinstrument'])
    elif market == 'czce':
        hq_df = get_czce_hq_by_date(date, category=category)
    elif market == 'cffex':
        hq_df = get_cffex_hq_by_date(date, category=category)
    else:
        log.info('Wrong exchange market name!')
        return pd.DataFrame()

    hq_df.to_csv(filepath, encoding='gb2312')
    return hq_df


def transfer_exchange_data(file_row, market='dce', category=0):
    """
    将每天的数据统一标准
    :param category: o 期货 1 期权
    :param market: 交易市场缩写
    :param file_row: collections.namedtuple,  a namedtuple for each row in the DataFrame
    :return: pd.DataFrame 统一标准后的数据
    """
    assert category in [0, 1]
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    file_path = file_row.filepath
    date = file_row.Index

    cols_path = HQ_COLUMNS_PATH[category]

    cols_df = pd.read_csv(cols_path, index_col=False, header=0,
                          usecols=['columns', market], encoding='gb2312')
    cols_df = cols_df.dropna()
    columns = cols_df[market].values  # 需要读取的数据列
    names = cols_df['columns'].values  # 对应的统一后的列的名称

    # dtype = dict(cols_df[[market, 'dtype']].values)  # 每列对应转换的数据类型
    if market == 'czce' and date < datetime(2010, 8, 24) and category == 0:
        cols_df.loc[cols_df['columns'] == 'volume', 'czce'] = '成交量'

    # 读取需转换的原始数据文件
    if market == 'dce':  # text
        hq_df = pd.read_csv(file_path, encoding='gb2312', header=0, index_col=False,
                            sep='\s+', thousands=',').dropna()
    elif market == 'shfe':  # json
        hq_df = pd.DataFrame(json.loads(file_path.read_text())['o_curinstrument'])
    else:
        hq_df = pd.read_csv(file_path, encoding='gb2312', header=0, index_col=False)

    if hq_df.empty:  # 原始数据文件为null，重新下载一次数据文件
        print('{} hq data is not exist, reload it!'.format(file_row.filepath))
        reload_hq_by_date(date, file_path, market=market, category=category)
        assert not hq_df.empty

    # 截取所需字段，并将字段名转换为统一标准
    hq_df = hq_df[cols_df[market].values]
    hq_df = hq_df.dropna()
    hq_df = hq_df[columns]
    hq_df.columns = names

    # 处理上海市场的空格和多余行
    if market == 'shfe':
        hq_df = hq_df[hq_df['settle'] != '']
        if category == 0:
            hq_df.loc[:, 'commodity'] = hq_df['commodity'].str.strip()
        else:
            hq_df.loc[:, 'symbol'] = hq_df['symbol'].str.strip()

    # 商品字母缩写转换和合约代码组织
    if category == 0:
        if market in ['shfe', 'dce']:
            code2name_table = CODE2NAME_TABLE.loc[CODE2NAME_TABLE['market'] == market, ['code', 'exchange']]
            code2name_table.set_index('exchange', inplace=True)
            hq_df = hq_df.join(code2name_table, on='commodity')
            del hq_df['commodity']
        elif market in ['czce', 'cffex']:
            hq_df['code'] = hq_df['symbol'].apply(lambda x: re.search('^[a-zA-Z]{1,2}', x)[0])
        else:
            log.info('Wrong exchange market name!')
            return pd.DataFrame()

        # 提取交割月份
        def convert_deliver(x):
            if not isinstance(x, str):
                x = str(x).strip()
            m = re.search('\d{3,4}$', x.strip())[0]
            return m if len(m) == 4 else '0{}'.format(m)

        if market in ['dce', 'czce']:  # cffex symbol不需要转换
            hq_df['symbol'] = hq_df['code'] + hq_df['symbol'].apply(convert_deliver)

    else:  # category = 1 期权
        hq_df.loc[:, 'symbol'] = hq_df['symbol'].str.upper() \
            .str.replace('-', '').str.strip()
        split_re = hq_df['symbol'].transform(
            lambda x: re.search('^([A-Z]{1,2})(\d{3,4})-?([A-Z])-?(\d+)', x))
        assert split_re is not None
        hq_df['code'] = split_re.transform(lambda x: x[1])
        hq_df['future_symbol'] = split_re.transform(
            lambda x: x[1] + (x[2] if len(x[2]) == 4 else '0{}'.format(x[2])))
        hq_df['type'] = split_re.transform(lambda x: x[3])
        hq_df['exeprice'] = pd.to_numeric(split_re.transform(lambda x: x[4]), downcast='float')

    # 数据类型转换，避免合并时出现错误
    hq_df['open'] = pd.to_numeric(hq_df['open'], downcast='float')
    hq_df['close'] = pd.to_numeric(hq_df['close'], downcast='float')
    hq_df['high'] = pd.to_numeric(hq_df['high'], downcast='float')
    hq_df['low'] = pd.to_numeric(hq_df['low'], downcast='float')
    hq_df['settle'] = pd.to_numeric(hq_df['settle'], downcast='float')

    hq_df['volume'] = pd.to_numeric(hq_df['volume'], downcast='integer')
    hq_df['openInt'] = pd.to_numeric(hq_df['openInt'], downcast='integer')

    # 行权量和delta只有期权独有
    if category == 1:
        hq_df['exevolume'] = pd.to_numeric(hq_df['exevolume'], downcast='integer')
        hq_df['delta'] = pd.to_numeric(hq_df['delta'], downcast='float')

    # 计算成交金额
    if market == 'shfe' and category == 0:  # 这样计算的不准确，只能近似
        hq_df['amount'] = pd.to_numeric(hq_df['volume'] * hq_df['close'], downcast='float')
    else:
        hq_df['amount'] = pd.to_numeric(hq_df['amount'], downcast='float') * 10000
    hq_df['datetime'] = date
    hq_df['market'] = market
    return hq_df


def get_hq_from_mongo(start, end, symbol, code, market='dce', category=0):
    """
    hdf5 文件知道如何插入记录，暂时只能添加在文件尾部，因此要保证 历史数据连续
    start=datetime(2005, 1, 2),
    :param market: ['dce', 'czce', 'shfe', 'cffex']
    :param category: 0 期货 1 期权
    :param end:
    :return:
    """
    assert category in [0, 1]
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    end = end if end else datetime.today()

    # 首先判断数据库是否更新
    db = connect_mongo(db='quote')

    if category == 0:
        cursor = db['future']
    else:
        cursor = db['option']

    target = RAW_HQ_DIR[category] / market

    # 交易日历中有而原始数据文件中没有
    file_index = get_download_file_index(target, start=start, end=end)

    if not file_index.empty:
        # TODO 直接传入需要下载数据的日期
        eval('get_{}_hq_by_dates(start=update, end=end, category=category)'.format(market))

    # TODO 查找需要插入数据库的数据
    update = None
    if target.exists():
        # 找最后一个记录的时间，所有商品相同
        last = pd.read_hdf(target, 'table', start=-1)
        update = last.index[0] + timedelta(1)

        if update > end:
            return

    # 保证每次将数据更新需要的程度
    eval('get_{}_hq_by_dates(start=update, end=end, category=category)'.format(market))

    # 查取需要读取的原始数据文件,原始数据文件必须以日期开头命名，前4位数字代表年份
    file_df = pd.DataFrame([(pd.to_datetime(re.search(DATE_PATTERN, x.name)[0]), x)
                            for x in source.glob('[0-9][0-9][0-9][0-9]*')], columns=['datetime', 'filepath'])
    file_df.set_index('datetime', inplace=True)

    if update:
        file_df.query("index>=Timestamp('{}') & index<=Timestamp('{}')".format(update, end),
                      inplace=True)

    if file_df.empty:
        return


if __name__ == '__main__':
    start_dt = datetime(2014, 12, 21)
    end_dt = datetime(2005, 1, 4)
    print(datetime.now())
    # get_czce_hq_by_date(end_dt)
    # get_dce_hq_by_dates(category=0)
    # get_shfe_hq_by_dates(category=0)
    # get_cffex_hq_by_dates(category=0)
    # construct_dce_hq(end=end_dt, category=0)
    # df = pd.read_csv(file_path, encoding='gb2312', sep='\s+')
    # df = get_future_hq('M', start=start_dt, end=None)
    from pathlib import Path
    from collections import namedtuple

    date = datetime(2018, 10, 25)
    filepath = Path(
        r'D:\Code\test\cookiercutter\datascience\datascinece\data\raw\future_option\shfe\{}_daily.txt'
            .format(date.strftime('%Y%m%d')))
    Pandas = namedtuple('Pandas', 'Index filepath')
    row = Pandas(Index=date, filepath=filepath)
    df = transfer_exchange_data(row, market='shfe', category=1)
    result = to_mongo('quote', 'option', df.to_dict('records'))
    print(datetime.now())

# -*- coding: utf-8 -*-
import json
import re

import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

from src.data.future.utils import get_file_index_needed
from src.data.setting import DATE_PATTERN
from src.log import LogHandler
from src.data.util.crawler import get_post_text, get_html_text
from src.data.future.setting import RAW_HQ_DIR, PROCESSED_HQ_DIR, CODE2NAME_PATH, HQ_COLUMNS_PATH

log = LogHandler('future.log')


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

    url_template = 'http://www.cffex.com.cn/sj/hqsj/rtj/{}/{}/{}_1.csv'
    url = url_template.format(date.strftime('%Y%m'), date.day, date.strftime('%Y%m%d'))

    return pd.read_csv(url, encoding='gb2312')


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

    file_index = get_file_index_needed(target, 'csv', start=start, end=end)

    if file_index.empty:
        return False

    for date in file_index:
        print(date)
        df = get_cffex_hq_by_date(date, category=category)
        date_str = date.strftime('%Y%m%d')
        file_path = target / '{}_daily.csv'.format(date_str)

        assert isinstance(df, pd.DataFrame)
        if df.empty:
            continue

        df.to_csv(file_path, encoding='gb2312')
        time.sleep(np.random.rand() * 90)
    return True


def get_czce_hq_by_date(date: datetime, category=0):
    """
    获取郑州商品交易所日交易数据
    http://www.czce.com.cn/cn/DFSStaticFiles/Future/2019/20190314/FutureDataDaily.txt
    http://www.czce.com.cn/cn/DFSStaticFiles/Future/2019/20190314/FutureDataDaily.htm
    期权 datetime(2017, 4, 19)
    http://www.czce.com.cn/cn/DFSStaticFiles/Option/2018/20180816/OptionDataDaily.htm
    http://www.czce.com.cn/cn/DFSStaticFiles/Option/2017/20171109/OptionDataDaily.htm
    datetime(2015, 9, 19)
    http://www.czce.com.cn/cn/exchange/2015/datadaily/20150821.htm
    datetime(2010, 8, 24)
    http://www.czce.com.cn/cn/exchange/jyxx/hq/hq20100806.html
    datetime(2005, 4, 29)

    :param date: datetime
    :param category: 行情类型, 0期货 或 1期权
    :return 不成功返回0

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    if date < datetime(2005, 4, 29):
        return pd.DataFrame()
    elif date < datetime(2010, 8, 24):
        url_template = 'http://www.czce.com.cn/cn/exchange/jyxx/hq/hq{}.html'
        url = url_template.format(date.strftime('%Y%m%d'))
    elif date < datetime(2015, 10, 8):
        url_template = 'http://www.czce.com.cn/cn/exchange/{}/datadaily/{}.htm'
        url = url_template.format(date.year, date.strftime('%Y%m%d'))
    else:
        template = ['http://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/{}/FutureDataDaily.htm',
                    'http://www.czce.com.cn/cn/DFSStaticFiles/Option/{}/{}/OptionDataDaily.htm']
        url_template = template[category]
        url = url_template.format(date.year, date.strftime('%Y%m%d'))

    text = get_html_text(url)

    if is_data_empty(date.strftime('%Y%m%d'), text):
        return pd.DataFrame()

    # if date < datetime(2008, 3, 19):
    #     attrs = {'width': '730'}
    # elif date < datetime(2010, 8, 24):
    #     attrs = {'width': '790'}
    # else:
    #     if category == 0 or date < datetime(2017, 12, 28):
    #         attrs = {'id': 'senfe'}
    #     else:
    #         attrs = {'id': 'tab1'}

    return pd.read_html(text, header=0)[1]


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

    file_index = get_file_index_needed(target, 'csv', start=start, end=end)

    if file_index.empty:
        return False

    for date in file_index:
        print(date)
        df = get_czce_hq_by_date(date, category=category)
        date_str = date.strftime('%Y%m%d')
        file_path = target / '{}_daily.csv'.format(date_str)
        if df.empty:
            continue
        df.to_csv(file_path, encoding='gb2312')
        time.sleep(np.random.rand() * 90)

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

    file_index = get_file_index_needed(target, 'txt', start=start, end=end)

    if file_index.empty:
        return False

    for date in file_index:
        print(date)
        text = get_shfe_hq_by_date(date, category=category)
        date_str = date.strftime('%Y%m%d')
        file_path = target / '{}_daily.txt'.format(date_str)

        if is_data_empty(date_str, text):
            continue

        file_path.write_text(text)
        time.sleep(np.random.rand() * 90)

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

    file_index = get_file_index_needed(target, 'txt', start=start, end=end)

    if file_index.empty:
        return False

    for date in file_index:
        print(date)
        text = get_dce_hq_by_date(date, category=category)
        date_str = date.strftime('%Y%m%d')
        file_path = target / '{}_daily.txt'.format(date_str)

        if is_data_empty(date_str, text):
            continue

        file_path.write_text(text)
        time.sleep(np.random.rand() * 90)

    return True


def transfer_exchange_data(row, market='dce', category=0):
    """
    将每天的数据统一标准存入中间数据文件
    :param category: o 期货 1 期权
    :param market: 交易市场缩写
    :param row: collections.namedtuple,  a namedtuple for each row in the DataFrame
    :return:
    """
    assert category in [0, 1]
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    cols_path = HQ_COLUMNS_PATH[category]

    cols_df = pd.read_csv(cols_path, index_col=False, header=0,
                          usecols=['columns', market, 'dtype'], encoding='gb2312')
    cols_df = cols_df.dropna()
    dtype = dict(cols_df[[market, 'dtype']].values)   # 每列对应转换的数据类型
    columns = cols_df[market].values   # 需要读取的数据列
    names = cols_df['columns'].values  # 对应的统一后的列的名称

    filepath = row.filepath
    date = row.Index

    if market == 'dce':   # text
        hq_df = pd.read_csv(filepath, encoding='gb2312', header=0, index_col=False,
                            dtype=dtype, sep='\s+', thousands=',').dropna()
    elif market == 'shfe':  # json
        hq_df = json.loads(filepath.read_text())['o_curinstrument']
        hq_df = hq_df[cols_df[market].values]
        # 转换数据类型
    else:
        hq_df = pd.read_csv(filepath, encoding='gb2312', header=0, index_col=False,
                            dtype=dtype).dropna()

    if hq_df.empty:  # 原始数据文件为null，重新下载一次数据文件 TODO 分市场讨论
        text = get_dce_hq_by_date(date, category=category)
        filepath.write_text(text)
        hq_df = pd.read_csv(filepath, encoding='gb2312', header=0, index_col=False,
                            dtype=dtype, sep='\s+', thousands=',').dropna()
        print('{} hq data is not exist, reload it!'.format(row.filepath))
        assert not hq_df.empty

    hq_df = hq_df[columns]
    hq_df.columns = names
    hq_df['datetime'] = date
    return hq_df


def construct_dce_hq(end=None, market='dce', category=0):
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

    end = datetime.today() if end is None else end

    # 商品中文名和字母缩写对照表
    df = pd.read_csv(CODE2NAME_PATH, encoding='gb2312', header=0,
                     usecols=['code', 'market', 'exchange']).dropna()

    df = df[df['market'] == market]
    del df['market']
    assert not df.empty

    source = RAW_HQ_DIR[category] / market

    filename = ['{}_future_hq.day'.format(market), '{}_option_hq.day'.format(market)]

    target = PROCESSED_HQ_DIR[category] / filename[category]
    target.parent.mkdir(parents=True, exist_ok=True)

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

    # 数据有错位和缺失值，转换int32会出错，后续计算也会出现浮点数
    dtype = [{'开盘价': 'float64', '最高价': 'float64', '最低价': 'float64', '收盘价': 'float64',
              '结算价': 'float64', '涨跌': 'float64', '涨跌1': 'float64', '成交量': 'float64',
              '持仓量': 'float64', '持仓量变化': 'float64', '成交额	': 'float64',
              "商品名称": 'object', "交割月份": 'object'},
             {'开盘价': 'float64', '最高价': 'float64', '最低价': 'float64', '收盘价': 'float64',
              '前结算价': 'float64', 'Delta': 'float64', '行权量': 'float64',
              '结算价': 'float64', '涨跌': 'float64', '涨跌1': 'float64', '成交量': 'float64',
              '持仓量': 'float64', '持仓量变化': 'float64', '成交额	': 'float64',
              "商品名称": 'object', "合约名称": 'object'}]
    # 读取数据转换内容

    frames = [transfer_exchange_data(x) for x in file_df.itertuples()]

    if len(frames) == 0:
        return

    spread_df = pd.concat(frames, ignore_index=True)

    df.set_index('exchange', inplace=True)
    spread_df = spread_df.join(df, on='商品名称')

    spread_df.set_index('日期', inplace=True)
    try:
        spread_df.to_hdf(target, 'table', format='table',
                         append=True, complevel=5, complib='blosc')
    except ValueError:  # TypeError
        log.warning('{}'.format(spread_df.columns))


if __name__ == '__main__':
    start_dt = datetime(2014, 12, 21)
    end_dt = datetime(2005, 1, 4)
    print(datetime.now())
    # get_czce_hq_by_date(end_dt)
    # get_dce_hq_by_dates(category=0)
    # get_shfe_hq_by_dates(category=1)
    get_dce_hq_by_dates(category=0)
    # construct_dce_hq(end=end_dt, category=0)
    # df = pd.read_csv(file_path, encoding='gb2312', sep='\s+')
    # df = get_future_hq('M', start=start_dt, end=None)
    print(datetime.now())

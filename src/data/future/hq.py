# -*- coding: utf-8 -*-
import json
import re
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.data.future.setting import HQ_COLUMNS_PATH, CODE2NAME_TABLE
from src.data.future.utils import get_download_file_index, get_insert_mongo_files
from src.data.setting import DATE_PATTERN, RAW_HQ_DIR, INSTRUMENT_TYPE
from src.data.util import get_post_text, get_html_text, connect_mongo, read_mongo, to_mongo
from src.log import LogHandler

TIME_WAITING = 3
log = LogHandler('future.hq.log')


def is_data_empty(data):
    """
    判断数据是否存在
    :param data: pd.DataFrame or str
    :return: True 数据不存在
    """
    if isinstance(data, pd.DataFrame):
        return data.empty
    elif not isinstance(data, str) or len(data) < 110:
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
    :return str

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    # url_template = 'http://www.cffex.com.cn/sj/hqsj/rtj/{}/{}/{}_1.csv'
    url_template = 'http://www.cffex.com.cn/fzjy/mrhq/{}/{}/{}_1.csv'
    url = url_template.format(date.strftime('%Y%m'), date.strftime('%d'), date.strftime('%Y%m%d'))
    # url_template = 'http://www.cffex.com.cn/sj/hqsj/rtj/{}/{}/index.xml'
    # url = url_template.format(date.strftime('%Y%m'), date.strftime('%d'))

    return get_html_text(url)


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
    :return pd.DataFrame

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

    if is_data_empty(text):
        return pd.DataFrame()

    tables = pd.read_html(text, header=0)

    df = tables[index]

    bflag = df.empty or len(df.columns) < 10 or len(df.columns) > 20

    if not bflag:
        return df

    # 处理特殊的例外情况 2017-12-27 index=3
    for df in tables:
        bflag = df.empty or len(df.columns) < 10 or len(df.columns) > 20
        if not bflag:
            return df

    return pd.DataFrame()


def get_shfe_hq_by_date(date: datetime, category=0):
    """
    获取上海商品交易所日交易数据 20020108/20090105 期货数据起始日（还可以往前取） 2018921 期权数据起始日
    http://www.shfe.com.cn/data/dailydata/kx/kx20190318.dat
    http://www.shfe.com.cn/data/dailydata/option/kx/kx20190315.dat
    :param date: datetime
    :param category: 行情类型, 0期货 或 1期权
    :return str

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    url_template = ['http://www.shfe.com.cn/data/dailydata/kx/kx{}.dat',
                    'http://www.shfe.com.cn/data/dailydata/option/kx/kx{}.dat']
    url = url_template[category].format(date.strftime('%Y%m%d'))

    return get_html_text(url)


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
    :return str

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


def get_hq_by_dates(market, category=0):
    """
    根据日期连续下载交易所日交易数据
    :param market:
    :param category: 行情类型, 0期货 或 1期权
    :return True False: 说明不用下载数据

    """
    assert category in [0, 1]
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    target = RAW_HQ_DIR[category] / market

    file_index = get_download_file_index(market, category)

    if file_index.empty:
        return False

    get_exchange_hq_func = {'cffex': get_cffex_hq_by_date,
                            'czce': get_czce_hq_by_date,
                            'shfe': get_shfe_hq_by_date,
                            'dce': get_dce_hq_by_date}

    for dt in file_index:
        print('{} downloading {} {} hq:{} data!'.format(
            datetime.now().strftime('%H:%M:%S'), market, dt.strftime('%Y-%m-%d'), category))
        data = get_exchange_hq_func[market](dt, category=category)
        date_str = dt.strftime('%Y%m%d')
        file_path = target / '{}.day'.format(date_str)

        if is_data_empty(data):
            log.warning('{} {} data:{} is not downloaded! '.format(market, date_str, category))
            time.sleep(np.random.rand() * TIME_WAITING * 3)
            continue

        if market == 'czce':
            data.to_csv(file_path, encoding='gb2312')
        else:
            file_path.write_text(data)
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
    if hq_df.empty:
        log.warning('{} instrument type {} data is still not exist!'.format(market, category))
    else:
        hq_df.to_csv(filepath, encoding='gb2312')
        print('Reload {} instrument type {} data successful!'.format(market, category))
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

    bflag = hq_df.empty or len(hq_df.columns) < len(columns) or len(hq_df.columns) > 20
    if bflag:  # 原始数据文件为null，重新下载一次数据文件
        print('{} instrument type:{} {} hq data is not exist, reload it!'.
              format(market, category, file_row.filepath.name))
        hq_df = reload_hq_by_date(date, file_path, market=market, category=category)
        bflag = hq_df.empty or len(hq_df.columns) < len(columns) or len(hq_df.columns) > 20
        if bflag:
            return hq_df

    # 截取所需字段，并将字段名转换为统一标准
    # hq_df = hq_df[cols_df[market].values]
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


def insert_hq_to_mongo():
    """
    下载数据文件，插入mongo数据库
    :return:
    """
    category = [0, 1]
    market = ['dce', 'czce', 'shfe', 'cffex']

    conn = connect_mongo(db='quote')

    # 首先尝试下载数据
    for c in category:
        t = INSTRUMENT_TYPE[c]
        cursor = conn[t]
        for m in market:
            # get_hq_by_dates(m, c)

            file_df = get_insert_mongo_files(m, c)

            if file_df.empty:
                print('{} instrument type:{} is updated before!'.format(m, t))
                continue

            for row in file_df.itertuples():
                df = transfer_exchange_data(row, m, c)
                if df.empty:
                    continue
                result = cursor.insert_many(df.to_dict('records'))
                if result is False:
                    print('{} {} {} insert failure.'.format(m, t, row.filepath.name))
                else:
                    print('{} {} {} insert success.'.format(m, t, row.filepath.name))
            print('{} instrument type:{} is updated now!'.format(m, t))


if __name__ == '__main__':
    # start_dt = datetime(2014, 12, 21)
    # end_dt = datetime(2005, 1, 4)
    print(datetime.now())
    # get_czce_hq_by_date(end_dt)
    # get_dce_hq_by_dates(category=0)
    # get_hq_by_dates('cffex', category=0)
    # get_cffex_hq_by_dates(category=0)
    # construct_dce_hq(end=end_dt, category=0)
    # df = pd.read_csv(file_path, encoding='gb2312', sep='\s+')
    # df = get_future_hq('M', start=start_dt, end=None)
    from pathlib import Path
    from collections import namedtuple

    # date = datetime(2018, 10, 25)
    # filepath = Path(
    #     r'D:\Code\test\cookiercutter\datascience\datascinece\data\raw\future_option\shfe\{}_daily.txt'
    #         .format(date.strftime('%Y%m%d')))
    # Pandas = namedtuple('Pandas', 'Index filepath')
    # row = Pandas(Index=date, filepath=filepath)
    # df = transfer_exchange_data(row, market='shfe', category=1)
    # result = to_mongo('quote', 'option', df.to_dict('records'))
    insert_hq_to_mongo()
    print(datetime.now())

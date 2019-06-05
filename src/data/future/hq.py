# -*- coding: utf-8 -*-
import json
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from pymongo import ASCENDING, DESCENDING

from src.data import conn
from src.data.setting import TRADE_BEGIN_DATE
from src.data.future.setting import NAME2CODE_MAP, COLUMNS_MAP
from src.data.future.utils import get_download_file_index, move_data_files, get_exist_files, \
    split_symbol
from src.data.setting import RAW_HQ_DIR, INSTRUMENT_TYPE
from src.util import get_post_text, get_html_text
from log import LogHandler

# TIME_WAITING = 1
log = LogHandler('data.log')


# ----------------------------------download data from web-----------------
def is_data_empty(data):
    """
    判断数据是否存在
    :param data: pd.DataFrame or str
    :return: True 数据不存在
    """
    if isinstance(data, pd.DataFrame):
        return data.empty
    elif not isinstance(data, str):
        return True
    elif re.search('doctype', data, re.I):
        return True
    elif len(data) < 100:
        return True
    else:
        return False


def download_cffex_hq_by_date(date: datetime, category=0):
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

    url_template = 'http://www.cffex.com.cn/fzjy/mrhq/{}/{}/{}_1.csv'
    url = url_template.format(date.strftime('%Y%m'), date.strftime('%d'), date.strftime('%Y%m%d'))

    return get_html_text(url)


def download_czce_hq_by_date(date: datetime, category=0):
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
    ret = pd.DataFrame()

    if date > datetime(2015, 10, 7):
        template = ['http://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/{}/FutureDataDaily.htm',
                    'http://www.czce.com.cn/cn/DFSStaticFiles/Option/{}/{}/OptionDataDaily.htm']
        url_template = template[category]
        url = url_template.format(date.year, date.strftime('%Y%m%d'))
    elif date > datetime(2010, 8, 23):
        url_template = 'http://www.czce.com.cn/cn/exchange/{}/datadaily/{}.htm'
        url = url_template.format(date.year, date.strftime('%Y%m%d'))
        index = 3
    elif date > datetime(2005, 4, 28):
        url_template = 'http://www.czce.com.cn/cn/exchange/jyxx/hq/hq{}.html'
        url = url_template.format(date.strftime('%Y%m%d'))
        index = 1
    else:
        return pd.DataFrame()

    text = get_html_text(url)

    if is_data_empty(text):
        return ret

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

    return ret


def download_shfe_hq_by_date(date: datetime, category=0):
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


def download_dce_hq_by_date(date: datetime, code='all', category=0):
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


def download_hq_by_date(date, file_path, market='dce', category=0):
    """
    从交易所网站获取某天的所有行情数据，存盘并返回pd.DataFrame
    :param date: 需要数据的日期
    :param file_path: 存储文件的地址
    :param market: 交易所代码
    :param category: 0:期货 1：期权
    :return: pd.DataFrame
    """
    assert category in [0, 1]
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    get_exchange_hq_func = {'cffex': download_cffex_hq_by_date,
                            'czce': download_czce_hq_by_date,
                            'shfe': download_shfe_hq_by_date,
                            'dce': download_dce_hq_by_date}

    data = get_exchange_hq_func[market](date, category=category)
    date_str = date.strftime('%Y%m%d')

    if is_data_empty(data):
        log.warning('{} {} data:{} is not downloaded! '.format(market, date_str, category))
        # time.sleep(np.random.rand() * TIME_WAITING * 3)
        return False

    if market == 'czce':
        data.to_csv(file_path, encoding='gb2312')
    else:
        file_path.write_text(data)

    return True


def download_hq_by_dates(market, start, category=0):
    """
    根据日期连续下载交易所日交易数据
    :param start:
    :param market:
    :param category: 行情类型, 0期货 或 1期权
    :return True False: 说明不用下载数据

    """
    assert category in [0, 1]
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    target = RAW_HQ_DIR[category] / market

    file_index = get_download_file_index(target, start=start)

    if file_index.empty:
        return False

    for dt in file_index:
        print('{} downloading {} {} hq:{} data!'.format(
            datetime.now().strftime('%H:%M:%S'), market, dt.strftime('%Y-%m-%d'), category))
        date_str = dt.strftime('%Y%m%d')
        file_path = target / '{}.day'.format(date_str)
        download_hq_by_date(dt, file_path, market, category)
        # time.sleep(np.random.rand() * TIME_WAITING)
    return True


def convert_deliver(symbol, date):
    """
    从合约代码中提取交割月份数据
    :param symbol: 
    :param date: 
    :return: 
    """

    if not isinstance(symbol, str):
        symbol = str(symbol).strip()
    m = re.search('\d{3,4}$', symbol.strip())[0]
    if len(m) == 4:
        return m

    if m[0] == '0':
        y = date.year
        y = int(y - np.floor(y / 100) * 100 + 1)
        m = str(y) + m[-2:]
    else:
        m = date.strftime('%y')[0] + m

    return m


def data_type_conversion(data, category, columns, names, date, market):
    """
    数据类型转换，将str转换为数字类型
    :param market:
    :param date:
    :param names:
    :param columns:
    :param category: 行情类型
    :param data:
    :return:
    """
    hq_df = data.copy()

    # 截取所需字段，并将字段名转换为统一标准
    hq_df = hq_df[columns]
    hq_df.columns = names
    hq_df = hq_df.dropna()

    hq_df['datetime'] = date
    hq_df['market'] = market

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

    return hq_df


def transfer_dce_future_hq(date, file_path, columns_map):
    """
    将每天的数据统一标准
    :return: pd.DataFrame 统一标准后的数据
    """
    ret = pd.DataFrame()

    hq_df = pd.read_csv(file_path, encoding='gb2312', header=0, index_col=False,
                        sep='\s+', thousands=',')

    bflag = hq_df.empty or len(hq_df.columns) < len(columns_map) or len(hq_df.columns) > 20
    if bflag:  # 原始数据文件为null，不重新下载，需要再运行一次程序
        print('dce future hq data:{} is not exist, please rerun program!'.format(file_path.name))
        return ret

    hq_df = data_type_conversion(hq_df, 0, list(columns_map.values()), list(columns_map.keys()), date, 'dce')

    # 商品字母缩写转换
    hq_df['code'] = hq_df['code'].transform(lambda x: NAME2CODE_MAP['exchange'][x])

    # 构建symbol
    hq_df['symbol'] = hq_df['code'] + hq_df['symbol'].transform(lambda x: convert_deliver(x, date))

    hq_df['amount'] = pd.to_numeric(hq_df['amount'], downcast='float') * 10000

    return hq_df


def transfer_czce_future_hq(date, file_path, columns_map):
    """
    将每天的数据统一标准
    :return: pd.DataFrame 统一标准后的数据
    """
    ret = pd.DataFrame()

    hq_df = pd.read_csv(file_path, encoding='gb2312', header=0, index_col=False)

    bflag = hq_df.empty or len(hq_df.columns) < len(columns_map) or len(hq_df.columns) > 20
    if bflag:  # 原始数据文件为null，不重新下载，需要再运行一次程序
        print('dce future hq data:{} is not exist, please rerun program!'.format(file_path.name))
        return ret

    columns_map = columns_map.copy()

    if date < datetime(2010, 8, 24):
        columns_map['volume'] = columns_map['volume'][0]
    else:
        columns_map['volume'] = columns_map['volume'][1]

    # 商品字母缩写转换
    symbol_name = columns_map['symbol']
    split_re, index = split_symbol('^[a-zA-Z]{1,2}', hq_df[symbol_name])
    hq_df = hq_df if all(index) else hq_df[index]  # 删除非数据行

    hq_df = data_type_conversion(hq_df, 0, list(columns_map.values()), list(columns_map.keys()), date, 'czce')

    # TODO 确认不需要keys，NAME2CODE_MAP['exchange'].keys()
    hq_df['code'] = split_re.transform(
        lambda x: NAME2CODE_MAP['exchange'][x[0]] if x[0] in NAME2CODE_MAP['exchange'].keys() else x[0])

    # 构建symbol
    hq_df['symbol'] = hq_df['code'] + hq_df['symbol'].transform(lambda x: convert_deliver(x, date))

    hq_df['amount'] = pd.to_numeric(hq_df['amount'], downcast='float') * 10000

    return hq_df


def transfer_shfe_future_hq(date, file_path, columns_map):
    """
    将每天的数据统一标准
    :return: pd.DataFrame 统一标准后的数据
    """
    ret = pd.DataFrame()

    data = json.loads(file_path.read_text())
    hq_df = pd.DataFrame(data['o_curinstrument'])
    total_df = pd.DataFrame(data['o_curproduct'])

    bflag = hq_df.empty or len(hq_df.columns) < len(columns_map) or len(hq_df.columns) > 20
    if bflag:  # 原始数据文件为null，不重新下载，需要再运行一次程序
        print('dce future hq data:{} is not exist, please rerun program!'.format(file_path.name))
        return ret

    settle_name = columns_map['settle']
    hq_df = hq_df[hq_df[settle_name] != '']

    hq_df = data_type_conversion(hq_df, 0, list(columns_map.values()), list(columns_map.keys()), date, 'shfe')
    hq_df.loc[:, 'code'] = hq_df['code'].str.strip()
    # 商品字母缩写转换
    hq_df['code'] = hq_df['code'].transform(lambda x: NAME2CODE_MAP['exchange'][x])

    # 构建symbol
    hq_df['symbol'] = hq_df['code'] + hq_df['symbol'].transform(lambda x: convert_deliver(x, date))

    # 计算amount
    total_df['PRODUCTNAME'] = total_df['PRODUCTNAME'].str.strip()

    total_df['AVGPRICE'] = pd.to_numeric(total_df['AVGPRICE'], downcast='float')
    total_df['VOLUME'] = pd.to_numeric(total_df['VOLUME'], downcast='integer')
    total_df['TURNOVER'] = pd.to_numeric(total_df['TURNOVER'], downcast='float')

    total_df = total_df[total_df['AVGPRICE'] > 0]

    total_df['code'] = total_df['PRODUCTNAME'].transform(lambda x: NAME2CODE_MAP['exchange'][x.strip()])

    total_df['multiplier'] = total_df['TURNOVER'] / total_df['AVGPRICE'] / total_df['VOLUME'] * 100000000
    total_df['multiplier'] = total_df['multiplier'].transform(round)

    hq_df = hq_df.join(total_df[['code', 'multiplier']].set_index('code'), on='code')
    hq_df['amount'] = hq_df['volume'] * hq_df['settle'] * hq_df['multiplier']
    del hq_df['multiplier']

    return hq_df


def transfer_cffex_future_hq(date, file_path, columns_map):
    """
    将每天的数据统一标准
    :return: pd.DataFrame 统一标准后的数据
    """
    ret = pd.DataFrame()

    hq_df = pd.read_csv(file_path, encoding='gb2312', header=0, index_col=False)

    bflag = hq_df.empty or len(hq_df.columns) < len(columns_map) or len(hq_df.columns) > 20
    if bflag:  # 原始数据文件为null，不重新下载，需要再运行一次程序
        print('cffex future hq data:{} is not exist, please rerun program!'.format(file_path.name))
        return ret

    # 商品字母缩写转换
    symbol_name = columns_map['symbol']
    hq_df[symbol_name] = hq_df[symbol_name].str.strip()  # cffex 下载数据中有空格
    split_re, index = split_symbol('^[a-zA-Z]{1,2}', hq_df[symbol_name])
    hq_df = hq_df if all(index) else hq_df[index]

    hq_df = data_type_conversion(hq_df, 0, list(columns_map.values()), list(columns_map.keys()), date, 'cffex')

    hq_df['code'] = split_re.transform(lambda x: x[0])
    hq_df['amount'] = pd.to_numeric(hq_df['amount'], downcast='float') * 10000

    return hq_df


def transfer_dce_option_hq(date, file_path, columns_map):
    """
    将每天的数据统一标准
    :return: pd.DataFrame 统一标准后的数据
    """
    ret = pd.DataFrame()

    hq_df = pd.read_csv(file_path, encoding='gb2312', header=0, index_col=False,
                        sep='\s+', thousands=',')

    bflag = hq_df.empty or len(hq_df.columns) < len(columns_map) or len(hq_df.columns) > 20
    if bflag:  # 原始数据文件为null，不重新下载，需要再运行一次程序
        print('dce future hq data:{} is not exist, please rerun program!'.format(file_path.name))
        return ret

    symbol_name = columns_map['symbol']
    # 商品字母缩写转换
    hq_df.loc[:, symbol_name] = hq_df[symbol_name].str.upper() \
        .str.replace('-', '').str.strip()

    pattern = '^([A-Z]{1,2})(\d{3,4})-?([A-Z])-?(\d+)'
    split_re, index = split_symbol(pattern, hq_df[symbol_name])
    hq_df = hq_df if all(index) else hq_df[index]

    hq_df = data_type_conversion(hq_df, 1, list(columns_map.values()), list(columns_map.keys()), date, 'dce')

    hq_df['code'] = split_re.transform(lambda x: x[1])
    hq_df['future_symbol'] = split_re.transform(
        lambda x: x[1] + (x[2] if len(x[2]) == 4 else '0{}'.format(x[2])))
    hq_df['type'] = split_re.transform(lambda x: x[3])
    hq_df['exeprice'] = pd.to_numeric(split_re.transform(lambda x: x[4]), downcast='float')

    hq_df['amount'] = pd.to_numeric(hq_df['amount'], downcast='float') * 10000
    # dce 没有隐含波动率数据

    return hq_df


def transfer_czce_option_hq(date, file_path, columns_map):
    """
    将每天的数据统一标准
    :return: pd.DataFrame 统一标准后的数据
    """
    ret = pd.DataFrame()

    hq_df = pd.read_csv(file_path, encoding='gb2312', header=0, index_col=False)

    bflag = hq_df.empty or len(hq_df.columns) < len(columns_map) or len(hq_df.columns) > 20
    if bflag:  # 原始数据文件为null，不重新下载，需要再运行一次程序
        print('dce future hq data:{} is not exist, please rerun program!'.format(file_path.name))
        return ret

    symbol_name = columns_map['symbol']
    # 商品字母缩写转换
    hq_df.loc[:, symbol_name] = hq_df[symbol_name].str.upper() \
        .str.replace('-', '').str.strip()

    pattern = '^([A-Z]{1,2})(\d{3,4})-?([A-Z])-?(\d+)'
    split_re, index = split_symbol(pattern, hq_df[symbol_name])
    hq_df = hq_df if all(index) else hq_df[index]

    hq_df = data_type_conversion(hq_df, 1, list(columns_map.values()), list(columns_map.keys()), date, 'czce')

    hq_df['code'] = split_re.transform(lambda x: x[1])
    hq_df['future_symbol'] = split_re.transform(
        lambda x: x[1] + (x[2] if len(x[2]) == 4 else '0{}'.format(x[2])))
    hq_df['type'] = split_re.transform(lambda x: x[3])
    hq_df['exeprice'] = pd.to_numeric(split_re.transform(lambda x: x[4]), downcast='float')

    hq_df['amount'] = pd.to_numeric(hq_df['amount'], downcast='float') * 10000
    hq_df['sigma'] = pd.to_numeric(hq_df['sigma'], downcast='float')

    return hq_df


def transfer_shfe_option_hq(date, file_path, columns_map):
    """
    将每天的数据统一标准
    :return: pd.DataFrame 统一标准后的数据
    """
    ret = pd.DataFrame()

    hq_df = pd.DataFrame(json.loads(file_path.read_text())['o_curinstrument'])

    bflag = hq_df.empty or len(hq_df.columns) < len(columns_map) or len(hq_df.columns) > 20
    if bflag:  # 原始数据文件为null，不重新下载，需要再运行一次程序
        print('dce future hq data:{} is not exist, please rerun program!'.format(file_path.name))
        return ret

    # 处理上海市场的空格和多余行
    symbol_name = columns_map['symbol']
    settle_name = columns_map['settle']
    hq_df = hq_df[hq_df[settle_name] != '']
    hq_df.loc[:, symbol_name] = hq_df[symbol_name].str.strip()

    # 商品字母缩写转换
    hq_df.loc[:, symbol_name] = hq_df[symbol_name].str.upper() \
        .str.replace('-', '').str.strip()

    pattern = '^([A-Z]{1,2})(\d{3,4})-?([A-Z])-?(\d+)'
    split_re, index = split_symbol(pattern, hq_df[symbol_name])
    hq_df = hq_df if all(index) else hq_df[index]

    hq_df = data_type_conversion(hq_df, 1, list(columns_map.values()), list(columns_map.keys()), date, 'shfe')

    hq_df['code'] = split_re.transform(lambda x: x[1])
    hq_df['future_symbol'] = split_re.transform(
        lambda x: x[1] + (x[2] if len(x[2]) == 4 else '0{}'.format(x[2])))
    hq_df['type'] = split_re.transform(lambda x: x[3])
    hq_df['exeprice'] = pd.to_numeric(split_re.transform(lambda x: x[4]), downcast='float')

    hq_df['amount'] = pd.to_numeric(hq_df['amount'], downcast='float') * 10000
    # hq_df['sigma'] = pd.to_numeric(hq_df['sigma'], downcast='float')

    return hq_df


def insert_hq_to_mongo():
    """
    下载数据文件，插入mongo数据库
    :return:
    """
    category = [0, 1]
    market = ['dce', 'czce', 'shfe', 'cffex']

    transfer_exchange_hq_func = [
        {
            'cffex': transfer_cffex_future_hq,
            'czce': transfer_czce_future_hq,
            'shfe': transfer_shfe_future_hq,
            'dce': transfer_dce_future_hq
        },
        {
            'czce': transfer_czce_option_hq,
            'shfe': transfer_shfe_option_hq,
            'dce': transfer_dce_option_hq
        }
    ]

    for c in category:
        t = INSTRUMENT_TYPE[c]
        cursor = conn[t]
        for m in market:
            if m == 'cffex' and c == 1:
                print("cffex has no option trading.")
                continue

            # if m in ['dce', 'czce', 'shfe'] or c == 1:
            #     print("debug.")
            #     continue
            # 下载更新行情的原始数据
            filer_dict = {"market": m}
            projection = {"_id": 0, "datetime": 1}

            start = cursor.find_one(filer_dict, projection=projection, sort=[("datetime", DESCENDING)])
            if start is None:
                start = TRADE_BEGIN_DATE[m][c]
            else:
                start = start['datetime'] + timedelta(1)
            download_hq_by_dates(m, start, c)

            # 需要导入数据库的原始数据文件
            # file_df = get_insert_mongo_files(m, c, start=start)
            file_df = get_exist_files(RAW_HQ_DIR[c] / m)
            file_df = file_df[start:]

            if file_df.empty:
                print('{} {} hq is updated before!'.format(m, t))
                continue
            columns_map = COLUMNS_MAP[t][m].copy()
            for row in file_df.itertuples():
                df = transfer_exchange_hq_func[c][m](row.Index, row.filepath, columns_map)
                if df.empty:
                    log.error("Transform {} {} {}data failure, please check program.".format(m, t, row.Index))
                    continue
                result = cursor.insert_many(df.to_dict('records'))
                if result:
                    print('{} {} {} insert success.'.format(m, t, row.filepath.name))
                    move_data_files(row.filepath)
                else:
                    print('{} {} {} insert failure.'.format(m, t, row.filepath.name))
            print('{} {} hq is updated now!'.format(m, t))


# ----------hq数据更新后更新index数据------------------
def build_weighted_index(hq_df, weight='volume'):
    """
    对行情数据求加权指数
    :param hq_df: pd.MultiIndex(datetime symbol)
    :param weight: str 权重指标 'volume', 'openInt'
    :return: hq_df 剔除了symbol字段或者索引
    """
    df = hq_df.copy()
    df = df.dropna()

    columns = ['open', 'high', 'low', 'close']

    for column in columns:
        df[column] = df[column] * df[weight]

    df = df.sum(level=0)

    for column in columns:
        df[column] = df[column] / df[weight]

    return df


def build_future_index():
    """
    编制指数数据：期货加权指数，主力合约指数，远月主力合约，交割主力合约
    对应的symbol-xx00:持仓量加权，xx11：成交量加权，xx88：主力合约，x99：远月合约，xx77:交割月合约
    按成交量对同一天的交易合约进行排序，取排名前三的交易合约，成交量最大的为主力合约
    最接近当月的合约为交割主力合约，在主力合约后交割的为远月主力合约
    :return:
    """
    # 更新数据库行情数据 独立运行，不在此处更新数据
    # insert_hq_to_mongo()

    # 连接数据库
    # conn = connect_mongo(db='quote')

    index_cursor = conn['index']
    hq_cursor = conn['future']

    # 从 future collection中提取60天内交易的品种
    filter_dict = {'datetime': {"$gt": datetime.today() - timedelta(360)}}
    codes = hq_cursor.distinct('code', filter_dict)
    if not isinstance(codes, list) or len(codes) == 0:
        print("Don't find any trading code in future collection!")
        return

    # 按品种分别编制指数
    for code in codes:
        # 获取指数数据最近的一条主力合约记录，判断依据是前一天的持仓量
        last_doc = index_cursor.find_one({'symbol': code + '88'}, sort=[('datetime', DESCENDING)])

        if last_doc:
            filter_dict = {'code': code, 'datetime': {'$gte': last_doc['datetime']}}
            # 已经改名交易品种['GN', 'WS', 'WT', 'RO', 'ER', 'ME', 'TC']
            #       老合约     新合约      老合约最后交易日
            # 甲醇   ME/50吨   MA/10吨       2015-5-15
            # 动力煤 TC/200吨  ZC/100吨      2016-4-8
            # 强筋小麦 WS/10吨  WH/20吨      2013-05-23
            # 硬白小麦 WT/10吨  PM/50吨      2012-11-22
            # 早籼稻  ER/10吨   RI/20吨      2013-5-23
            # 绿豆    GN                    2010-3-23
            # 菜籽油   RO/5吨   OI/10吨      2013-5-15
            # if code in ['GN', 'WS', 'WT', 'RO', 'ER', 'ME', 'TC']:
            #     print('{} is the {} last trading day.'.format(last_doc['datetime'].strftime('%Y-%m-%d'), code))
            #     continue
            # else:
            #     print("Build {} future index from {}".format(code, last_doc['datetime']))
        else:  # 测试指定日期
            # filter_dict = {'code': code, 'datetime': {'$lte': datetime(2003, 1, 1)}}
            filter_dict = {'code': code}
            print("Build {} future index from trade beginning.".format(code))

        # 从数据库读取所需数据
        hq = hq_cursor.find(filter_dict, {'_id': 0}).sort([("datetime", ASCENDING)])
        hq_df = pd.DataFrame(list(hq))
        if hq_df.empty:
            print('{} index data have been updated before!'.format(code))
            continue

        hq_df.set_index(['datetime', 'symbol'], inplace=True)
        # 需要按照索引排序

        date_index = hq_df.index.levels[0]
        if len(date_index) < 2:  # 新的数据
            print('{} index data have been updated before!'.format(code))
            continue

        index_names = ['domain', 'near', 'next']
        contract_df = pd.DataFrame(index=date_index, columns=index_names)

        for date in date_index:
            # hq.py:493: PerformanceWarning: indexing past lexsort depth may impact performance.
            #   s = hq_df.loc[date, 'openInt'].copy()
            s = hq_df.loc[date, 'openInt'].copy()
            s.sort_values(ascending=False, inplace=True)
            s = s[:min(3, len(s))]  # 预防合约小于3的情况,避免出现交割月和主力合约重合，主力合约和下月合约重合
            domain = s.index[0]
            contract_df.loc[date, 'domain'] = domain
            contract_df.loc[date, 'near'] = s.index.min()
            try:
                if s.index[1] > domain:
                    contract_df.loc[date, 'next'] = s.index[1]
                elif s.index[2] > domain:
                    contract_df.loc[date, 'next'] = s.index[2]
                else:
                    contract_df.loc[date, 'next'] = domain
            except IndexError:
                print("{} domain contract is the last contract {}".format(code, domain))
                contract_df.loc[date, 'next'] = domain

        pre_contract_df = contract_df.shift(1).dropna()
        # length = len(contract_df)
        pre_no_index_df = pre_contract_df.reset_index()
        hq_df = hq_df.loc[pre_no_index_df.datetime[0]:]  # 期货指数数据从第二个交易日开始

        frames = []

        index_symbol = [code + x for x in ['00', '11', '88', '77', '99']]
        multi_index_names = ['datetime', 'symbol']
        # 主力，交割，远月合约数据
        for name, symbol in zip(index_names, index_symbol[-3:]):

            multi_index = pd.MultiIndex.from_frame(
                pre_no_index_df[['datetime', name]], names=multi_index_names)
            index_diff = multi_index.difference(hq_df.index)

            # 头一天还有交割仓位，第二天合约消失的情况
            if not index_diff.empty:
                date_index = index_diff.get_level_values(level=0)
                pre_contract_df.loc[date_index, name] = contract_df.loc[date_index, name]
                pre_no_index_df = pre_contract_df.reset_index()
                multi_index = pd.MultiIndex.from_frame(
                    pre_no_index_df[['datetime', name]], names=multi_index_names)
                print('{} use {} current day contract'.format(symbol, len(index_diff)))

            index_df = hq_df.loc[multi_index]
            index_df.reset_index(inplace=True)
            index_df['contract'] = index_df['symbol']
            index_df['symbol'] = symbol
            frames += index_df.to_dict('records')

        # 加权指数
        for symbol, weight_name in zip(index_symbol[:2], ['openInt', 'volume']):
            index_df = build_weighted_index(hq_df, weight=weight_name)
            index_df.reset_index(inplace=True)
            index_df['code'] = code
            index_df['market'] = hq_df.market[0]
            index_df['symbol'] = symbol
            frames += index_df.to_dict('records')

        result = index_cursor.insert_many(frames)
        if result.acknowledged:
            print('{} index data insert success.'.format(code))
        else:
            print('{} index data insert failure.'.format(code))


if __name__ == '__main__':
    print(datetime.now())
    # start_dt = datetime(2014, 12, 21)
    # end_dt = datetime(2005, 1, 4)

    # download_czce_hq_by_date(end_dt)
    # get_dce_hq_by_dates(category=0)
    # download_hq_by_dates('cffex', category=0)
    # get_cffex_hq_by_dates(category=0)
    # construct_dce_hq(end=end_dt, category=0)
    # df = pd.read_csv(file_path, encoding='gb2312', sep='\s+')
    # df = get_future_hq('M', start=start_dt, end=None)

    # date = datetime(2018, 10, 25)
    # filepath = Path(
    #     r'D:\Code\test\cookiercutter\datascience\datascinece\data\raw\future_option\shfe\{}_daily.txt'
    #         .format(date.strftime('%Y%m%d')))
    # Pandas = namedtuple('Pandas', 'Index filepath')
    # row = Pandas(Index=date, filepath=filepath)
    # df = transfer_exchange_data(row, market='shfe', category=1)
    # result = to_mongo('quote', 'option', df.to_dict('records'))
    insert_hq_to_mongo()
    build_future_index()
    print(datetime.now())

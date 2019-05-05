# -*- coding: utf-8 -*-
import pandas as pd
from pymongo import ASCENDING

from src.util import connect_mongo
from src.api.cons import FREQ
from src.setting import DATA_ANALYST, ANALYST_PWD
from log import LogHandler

log = LogHandler('api.log')


def get_trading_dates(start_date, end_date, country='cn'):
    """

    :param start_date:    datetime.datetime, 开始日期
    :param end_date:      datetime.datetime, 结束日期
    :param country:       默认是中国市场('cn')，目前仅支持中国市场
    :return:    datetime.datetime list - 交易日期列表
    """
    pass


def get_previous_trading_date(date, n=1, country='cn'):
    """

    :param date:        datetime.datetime    指定日期
    :param n:           n代表往前第n个交易日。默认为1，即前一个交易日
    :param country:     默认是中国市场('cn')，目前仅支持中国市场
    :return:            datetime.datetime - 交易日期
    """
    pass


def get_next_trading_date(date, n, country='cn'):
    """

        :param date:        datetime.datetime    指定日期
        :param n:           n代表往后第n个交易日。默认为1，即前一个交易日
        :param country:     默认是中国市场('cn')，目前仅支持中国市场
        :return:            datetime.datetime - 交易日期
        """
    pass


def get_price(symbol=None, instrument='index', start_date=None, end_date=None, frequency='d', fields=None):
    """
        获取行情数据
    :param symbol: 合约代码，symbol, symbol list, 只支持同种类。获取tick数据时，只支持单个symbol
    :param instrument:   行情数据类型 ['future', 'option', 'stock', 'bond', 'convertible'] 以及 index
    :param start_date:
    :param end_date:    结束日期，交易使用时，默认为策略当前日期前一天
    :param frequency:   历史数据的频率, 默认为'd', 只支持日线级别以上数据。'5m'代表5分钟线。可支持期货tick级别数据获取，此时频率为'tick'
    :param fields:      字段名称
    :return:
        传入一个symbol，多个fields，函数会返回一个pandas DataFrame
        传入一个symbol，一个field，函数会返回pandas Series
        传入多个symbol，一个field，函数会返回一个pandas DataFrame
        传入多个symbol，函数会返回一个multiIndex DataFrame
    """
    # 连接数据库
    conn = connect_mongo(db='quote', username=DATA_ANALYST, password=ANALYST_PWD)

    cursor = conn[instrument]

    filter_dict = {}
    if isinstance(symbol, list):
        filter_dict['symbol'] = {'$in': symbol}
    elif isinstance(symbol, str):
        filter_dict['symbol'] = symbol
    else:
        log.debug('Return all commodities hq!')

    if start_date is not None:
        filter_dict['datetime'] = {'$gte': start_date}

    if end_date is not None:
        if 'datetime' in filter_dict:
            filter_dict['datetime']['$lte'] = end_date
        else:
            filter_dict['datetime'] = {'$lte': end_date}

    project_dict = {'_id': 0}
    if isinstance(fields, str):
        project_dict.update({'datetime': 1, fields: 1, 'symbol': 1})
    elif isinstance(fields, list):
        project_dict['datetime'] = 1
        project_dict.update({x: 1 for x in fields})
        project_dict['symbol'] = 1

    hq = cursor.find(filter_dict, project_dict).sort([("datetime", ASCENDING)])

    # Expand the cursor and construct the DataFrame
    hq_df = pd.DataFrame(list(hq))
    return hq_df


def get_blocks(symbol=None, start_date=None, end_date=None, frequency='d'):
    """
        获取行情数据
    :param symbol: 合约代码，symbol, symbol list, 只支持同种类.
    :param start_date:
    :param end_date:    结束日期，交易使用时，默认为策略当前日期前一天
    :param frequency:   历史数据的频率, 默认为'd', 只支持日线级别以上数据。'5m'代表5分钟线。    :return:
        传入一个symbol，多个fields，函数会返回一个pandas DataFrame
        传入一个symbol，一个field，函数会返回pandas Series
        传入多个symbol，一个field，函数会返回一个pandas DataFrame
        传入多个symbol，函数会返回一个multiIndexe DataFrame
    """
    # 连接数据库
    conn = connect_mongo(db='quote', username=DATA_ANALYST, password=ANALYST_PWD)

    cursor = conn['block']

    frequency = FREQ.index(frequency)

    filter_dict = {'frequency': frequency}

    if isinstance(symbol, list):
        filter_dict['symbol'] = {'$in': symbol}
    elif isinstance(symbol, str):
        filter_dict['symbol'] = symbol
    else:
        log.debug('Return all commodities blocks!')

    if start_date is not None:
        filter_dict['datetime'] = {'$gte': start_date}

    if end_date is not None:
        if 'datetime' in filter_dict:
            filter_dict['enter_date']['$lte'] = end_date
        else:
            filter_dict['enter_date'] = {'$lte': end_date}

    project_dict = {'_id': 0}

    blocks = cursor.find(filter_dict, project_dict)

    # Expand the cursor and construct the DataFrame
    block_df = pd.DataFrame(list(blocks))
    return block_df


def get_segments(symbol=None, start_date=None, end_date=None, frequency='d'):
    """
        获取行情数据
    :param symbol: 合约代码，symbol, symbol list, 只支持同种类.
    :param start_date:
    :param end_date:    结束日期，交易使用时，默认为策略当前日期前一天
    :param frequency:   历史数据的频率, 默认为'd', 只支持日线级别以上数据。'5m'代表5分钟线。    :return:
        传入一个symbol，多个fields，函数会返回一个pandas DataFrame
        传入一个symbol，一个field，函数会返回pandas Series
        传入多个symbol，一个field，函数会返回一个pandas DataFrame
        传入多个symbol，函数会返回一个multiIndexe DataFrame
    """
    # 连接数据库
    conn = connect_mongo(db='quote', username=DATA_ANALYST, password=ANALYST_PWD)

    cursor = conn['segment']

    frequency = FREQ.index(frequency)

    filter_dict = {'frequency': {'$gte': frequency}}

    if isinstance(symbol, list):
        filter_dict['symbol'] = {'$in': symbol}
    elif isinstance(symbol, str):
        filter_dict['symbol'] = symbol
    else:
        log.debug('Return all commodities segments!')

    if start_date is not None:
        filter_dict['datetime'] = {'$gte': start_date}

    if end_date is not None:
        if 'datetime' in filter_dict:
            filter_dict['datetime']['$lte'] = end_date
        else:
            filter_dict['datetime'] = {'$lte': end_date}

    project_dict = {'_id': 0}

    segments = cursor.find(filter_dict, project_dict)

    # Expand the cursor and construct the DataFrame
    segment_df = pd.DataFrame(list(segments))
    return segment_df


if __name__ == '__main__':
    symbols = ['A88', 'Y88', 'ME88']
    # start_dates = get_peak_start_date(symbol=symbols)

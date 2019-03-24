# -*- coding: utf-8 -*-
import re

import pandas as pd

from src.data.setting import DATE_PATTERN, INSTRUMENT_TYPE, TRADE_BEGIN_DATE, RAW_HQ_DIR
from src.data.util import connect_mongo
from src.log import LogHandler
from src.data.tdx import get_future_hq

log = LogHandler('future.log')


# 期货历史交易日历，返回铜期货指数交易日期，2000/1/4开始
def get_future_calender(start=None, end=None):
    df = get_future_hq('cuL9', start=start, end=end)
    if df is None:
        df = get_future_hq('cuL9', start=start)
    return df.index


def get_download_file_index(market, category):
    """
    计算需要下载的文件
    :param market:
    :param category:
    :return: pandas.core.indexes.datetimes.DatetimeIndex 日期的索引值
    """
    start = TRADE_BEGIN_DATE[market][category]

    ret = pd.to_datetime([])

    try:
        trade_index = get_future_calender(start=start)
    except AttributeError:
        log.info('From {} to today are not in trading calender!'.format(start))
        return ret

    file_index = get_exist_files(market, category).index

    if file_index.empty:
        file_index = trade_index
    else:
        file_index = trade_index.difference(file_index)

    return file_index


def get_insert_mongo_files(market, category):
    """
    计算需要插入数据的文件
    :param market:
    :param category:
    :return:
    pandas.DataFrame
        datetime: index
        filepath: pathlib.Path
    """
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    conn = connect_mongo(db='quote')
    cursor = conn[INSTRUMENT_TYPE[category]]

    date_index = cursor.distinct('datetime', {'market': market})

    if not date_index:
        return pd.DataFrame()

    file_df = get_exist_files(market, category)
    mongo_index = pd.datetime(date_index)
    file_index = file_df.index.difference(mongo_index)
    file_df = file_df.loc[file_index]

    return file_df


def get_exist_files(market, category):
    """
    计算需要下载的文件
    :param market:
    :param category:
    :return:
    pandas.DataFrame
        datetime: index
        filepath: pathlib.Path
    """
    source = RAW_HQ_DIR[category] / market
    ret = pd.DataFrame()

    if not source.exists():
        source.parent.mkdir(parents=True, exist_ok=True)
        source.mkdir(parents=True, exist_ok=True)
        file_df = ret
    else:
        try:
            file_df = pd.DataFrame([(pd.to_datetime(re.search(DATE_PATTERN, x.name)[0]), x)
                                    for x in source.glob('[0-9][0-9][0-9][0-9]*')], columns=['datetime', 'filepath'])
            file_df.set_index('datetime', inplace=True)
        except TypeError:  # 目录存在但没有文件
            file_df = ret

    return file_df


if __name__ == '__main__':
    from datetime import datetime

    get_future_calender(datetime(2008, 12, 3), datetime(2009, 1, 8))

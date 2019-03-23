# -*- coding: utf-8 -*-
import re

import pandas as pd
from datetime import datetime

from src.data.setting import DATE_PATTERN
from src.log import LogHandler
from src.data.tdx import get_future_hq

log = LogHandler('future.log')


# 期货历史交易日历，返回铜期货指数交易日期，2000/1/4开始
def get_future_calender(start=None, end=None):
    df = get_future_hq('cuL9', start=start, end=end)
    if df is None:
        df = get_future_hq('cuL9', start=start)
    return df.index


def get_download_file_index(target, start=None, end=None):
    """
    计算需要下载的文件
    :param target: 数据目录
    :param start: 需要数据的起始日期
    :param end:
    :return: pandas.core.indexes.datetimes.DatetimeIndex 日期的索引值
    """
    assert start <= end <= datetime.today()

    ret = pd.to_datetime([])

    try:
        trade_index = get_future_calender(start=start, end=end)
    except AttributeError:
        log.info('{} to {} are not in trading calender!'.format(start, end))
        return ret

    file_df = get_exist_files(target).index

    if file_df.empty:
        file_index = trade_index
    else:
        file_index = trade_index.difference(file_df.index)

    return file_index


def get_insert_files(source, cursor, market):
    """
    计算需要下载的文件
    :param market:
    :param cursor:
    :param source: 原始下载数据目录
    :return: pandas.core.indexes.datetimes.DatetimeIndex 日期的索引值
    """
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    mongo_index = pd.datetime(cursor.distinct('datetime', {'market': market}))

    file_df = get_exist_files(source)

    if mongo_index:
        file_index = file_df.index.difference(mongo_index)

    return file_index


def get_exist_files(source):
    """
    计算需要下载的文件
    :param source: 数据目录
    :return:
    pandas.DataFrame
        datetime: index
        filepath: pathlib.Path
    """
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

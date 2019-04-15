# -*- coding: utf-8 -*-
import re
from datetime import datetime, timedelta

import pandas as pd

from src.data.setting import DATE_PATTERN, INSTRUMENT_TYPE, RAW_HQ_DIR
from src.data.tdx import get_future_hq
from src.util import connect_mongo
from log import LogHandler

log = LogHandler('future.log')


# 期货历史交易日历，返回铜期货指数交易日期，2000/1/4开始
def get_future_calender(start=None, end=None, country='cn'):
    """

    :param start:
    :param end:
    :param country: 默认是中国市场('cn')，目前仅支持中国市场
    :return: pd.Index
    """
    # if start > datetime.today():
    #     return pd.DatetimeIndex(freq='1D')
    df = get_future_hq('cuL9', start=start, end=end)
    if df is None:
        df = get_future_hq('cuL9', start=start)
    return df.index


def get_download_file_index(market, category, start=datetime(2019, 1, 1)):
    """
    计算需要下载的文件
    :param start:    从某个交易日开始下载数据
    :param market:
    :param category:
    :return: pandas.core.indexes.datetimes.DatetimeIndex 日期的索引值
    """
    # start = TRADE_BEGIN_DATE[market][category]
    # start = datetime(2019, 1, 1)   # 数据已经确定下载不再需要比对
    ret = pd.to_datetime([])

    try:
        # 当天 5点后才下载当天数据
        today = datetime.today()
        year, month, day = today.year, today.month, today.day
        if datetime.now() > datetime(year, month, day, 16, 30):
            trade_index = get_future_calender(start=start)
        else:
            trade_index = get_future_calender(start=start, end=today - timedelta(1))
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

    # TODO 是否需要维护一个装门的数据list
    date_index = cursor.distinct('datetime', {'market': market})

    file_df = get_exist_files(market, category)

    if len(date_index) == 0:
        return file_df

    mongo_index = pd.to_datetime(date_index)
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

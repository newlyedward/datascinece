# -*- coding: utf-8 -*-
import re
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

from src.data import conn
from src.data import DATE_PATTERN, INSTRUMENT_TYPE, RAW_HQ_DIR, BACKUP_DIR
from src.data import get_future_hq
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
    # if start_date > datetime.today():
    #     return pd.DatetimeIndex(freq='1D')
    df = get_future_hq('cuL9', start=start, end=end)
    if df is None:
        df = get_future_hq('cuL9', start=start)
    return df.index


def get_download_file_index(target, start=datetime(2019, 4, 1)):
    """
    计算需要下载的文件
    :param target:
    :param start:    从某个交易日开始下载数据
    :return: pandas.core.indexes.datetimes.DatetimeIndex 日期的索引值
    """
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

    file_index = get_exist_files(target).index

    if file_index.empty:
        file_index = trade_index
    else:
        file_index = trade_index.difference(file_index)

    return file_index


def get_insert_mongo_files(market, category, start=datetime(2000, 1, 1)):
    """
    计算需要插入数据的文件
    :param start:
    :param market:
    :param category:
    :return:
    pandas.DataFrame
        datetime: index
        filepath: pathlib.Path
    """
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    # conn = connect_mongo(db='quote')
    cursor = conn[INSTRUMENT_TYPE[category]]
    start += timedelta(1)

    date_index = cursor.distinct('datetime', {'market': market, 'datetime': {'&gte': start}})

    data_dir = RAW_HQ_DIR[category] / market

    file_df = get_exist_files(data_dir)
    file_df = file_df[start:]

    if len(date_index) == 0:
        return file_df

    mongo_index = pd.to_datetime(date_index)
    file_index = file_df.index.difference(mongo_index)
    file_df = file_df.loc[file_index]

    return file_df


def get_exist_files(data_dir):
    """
    计算需要下载的文件
    :param data_dir: 数据文件存放目录，以日期格式命名的文件。
    :return:
    pandas.DataFrame
        datetime: index
        filepath: pathlib.Path
    """
    ret = pd.DataFrame()

    if not data_dir.exists():
        data_dir.parent.mkdir(parents=True, exist_ok=True)
        data_dir.mkdir(parents=True, exist_ok=True)
        file_df = ret
    else:
        try:
            file_df = pd.DataFrame([(pd.to_datetime(re.search(DATE_PATTERN, x.name)[0]), x)
                                    for x in data_dir.glob('[0-9][0-9][0-9][0-9]*')], columns=['datetime', 'filepath'])
            file_df.set_index('datetime', inplace=True)
        except TypeError:  # 目录存在但没有文件
            file_df = ret

    return file_df


def move_data_files(src, dst=BACKUP_DIR):
    if isinstance(src, str):
        src = Path(str)

    parts = src.parts
    dst = Path(dst + '\\'.join(parts[parts.index('data'):]))

    if dst.exists():
        return True

    parent = dst.parent

    if not parent.exists():
        parent.mkdir(parents=True)

    src.rename(dst)


def split_symbol(pattern, s):
    """
    对合约代码分析，并用正则表达式进行提取
            期货：商品代码
            期权：商品代码，期货合约代码，期权类型和行权价格
    :param pattern: 正则表达式
    :param s: symbol columns
    :return:
        pd.Series, idx 提取出信息对应的索引bool值
    """
    assert isinstance(s, pd.Series)
    #
    split_s = s.transform(lambda x: re.search(pattern, str(x)))
    idx = ~split_s.isna().values
    if not idx.all():
        split_s = split_s.dropna()
        log.debug("There are some Nan in re search!")
    return split_s, idx


if __name__ == '__main__':
    from datetime import datetime

    get_future_calender(datetime(2008, 12, 3), datetime(2009, 1, 8))

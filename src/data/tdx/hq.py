# -*- coding: utf-8 -*-
import os
import datetime as dt
import numpy as np
import pandas as pd

from src.log import LogHandler

from src.data.tdx.setting import tdx_dir, MARKET2TDX_CODE, MARKET_DIR, PERIOD_DIR, PERIOD_EXT

log = LogHandler(os.path.basename('tdx.hq.log'))


def int2date(x):
    year = int(x / 2048) + 2004
    month = int(x % 2048 / 100)
    day = x % 2048 % 100
    return dt.datetime(year, month, day)


def _get_future_day_hq(file_handler, count=-1):
    names = 'datetime', 'open', 'high', 'low', 'close', 'openInt', 'volume', 'comment'
    offsets = tuple(range(0, 31, 4))
    formats = 'i4', 'f4', 'f4', 'f4', 'f4', 'i4', 'i4', 'i4'

    dt_types = np.dtype({'names': names, 'offsets': offsets, 'formats': formats}, align=True)
    hq_day_df = pd.DataFrame(np.fromfile(file_handler, dtype=dt_types, count=count))
    hq_day_df.index = pd.to_datetime(hq_day_df['datetime'].astype('str'), errors='coerce')
    hq_day_df.pop('datetime')
    return hq_day_df


def _get_future_min_hq(file_handler, count=-1):
    names = 'date', 'time', 'open', 'high', 'low', 'close', 'openInt', 'volume', 'comment'
    formats = 'u2', 'u2', 'f4', 'f4', 'f4', 'f4', 'i4', 'i4', 'i4'
    offsets = (0, 2) + tuple(range(4, 31, 4))

    dt_types = np.dtype({'names': names, 'offsets': offsets, 'formats': formats}, align=True)
    hq_min_df = pd.DataFrame(np.fromfile(file_handler, dtype=dt_types, count=count))

    hq_min_df.index = hq_min_df.date.transform(int2date) + pd.to_timedelta(hq_min_df.time, unit='m')
    hq_min_df.pop('date')
    hq_min_df.pop('time')
    return hq_min_df


def get_future_day_hq(market, code, start=None, end=None):
    """
    :param market: 交易市场
    :param code: IL8 主力合约 IL9 期货指数 I1801
    :param start: 开始日期
    :param end:   结束日期
    :return: pd.DateFrame
    """

    tdx_hq_dir = os.path.join(tdx_dir, 'vipdoc', MARKET_DIR[market], PERIOD_DIR['d'])
    hq_filename = MARKET2TDX_CODE[market] + '#' + code.upper() + PERIOD_EXT['d']
    hq_path = os.path.join(tdx_hq_dir, hq_filename)

    if not os.path.exists(hq_path):
        return None

    f = open(hq_path, "rb")

    f.seek(0, 0)
    start_dt = np.fromfile(f, dtype=np.int32, count=1)
    start_dt = dt.datetime.strptime(start_dt.astype(str)[0], '%Y%m%d')

    f.seek(-32, 2)
    end_dt = np.fromfile(f, dtype=np.int32, count=1)
    end_dt = dt.datetime.strptime(end_dt.astype(str)[0], '%Y%m%d')

    if not start:
        start = dt.datetime(1970, 1, 1)

    if start < start_dt:
        f.seek(0, 0)
        return _get_future_day_hq(f)
    elif start > end_dt:
        return None

    delta = (end_dt - start)
    factor = delta.days
    try:
        f.seek(-32 * factor, 2)
    except OSError:
        f.seek(0, 0)
        log.warning('%s trade recodes are few and factor = %d is too big.', code, factor)
    hq_day_df = _get_future_day_hq(f)

    if end:
        return hq_day_df[end > hq_day_df.index > start]
    else:
        return hq_day_df[hq_day_df.index > start]


def get_future_min_hq(market, code, start=None, end=None, freq='5m'):
    """
    :param market: 交易市场
    :param code: IL8 主力合约 IL9 期货指数 I1801
    :param start: 开始时间
    :param end:   结束时间
    :param freq: 周期'1m'，'5m'
    :return: 返回
    """
    tdx_hq_dir = os.path.join(tdx_dir, 'vipdoc', MARKET_DIR[market], PERIOD_DIR[freq])
    hq_filename = MARKET2TDX_CODE[market] + '#' + code.upper() + PERIOD_EXT[freq]
    hq_path = os.path.join(tdx_hq_dir, hq_filename)

    if not os.path.exists(hq_path):
        return None

    f = open(hq_path, "rb")

    f.seek(0, 0)
    start_dt = np.fromfile(f, dtype=np.int16, count=1)
    start_dt = int2date(start_dt)

    f.seek(-32, 2)
    end_dt = np.fromfile(f, dtype=np.int16, count=1)
    end_dt = int2date(end_dt)

    if not start:
        start = dt.datetime(1970, 1, 1)

    if start < start_dt:
        f.seek(0, 0)
        return _get_future_min_hq(f)
    elif start > end_dt:
        return None

    k_num = 400  # 每天大多数期货交易的时间   9:00-10:15 10:30-11:30 13:30-15:00 21:00-23:30
    if freq == '5m':
        k_num = int(k_num / 5)

    delta = (end_dt - start)
    factor = delta.days * k_num

    while start < end_dt:
        try:
            f.seek(-32 * factor, 2)
            end_dt = np.fromfile(f, dtype=np.int16, count=1)
            f.seek(-32 * factor, 2)  # 数据读取后移位，文件指针要回到原来位置
            end_dt = int2date(end_dt)
            factor = factor * 2
        except OSError:
            f.seek(0, 0)
            log.warning('%s trade recodes are few and factor = %d is too big.', code, factor)
            break

    hq_min_df = _get_future_min_hq(f)
    if end:
        return hq_min_df[end > hq_min_df.index > start]
    else:
        return hq_min_df[hq_min_df.index > start]


if __name__ == '__main__':
    start = dt.datetime(2019, 2, 20)
    code = 'srl8'
    df = get_future_min_hq(market='czce', start=start, code=code, freq='5m')

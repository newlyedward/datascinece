# -*- coding: utf-8 -*-
import re

import pandas as pd
from datetime import datetime, timedelta

from pathlib import Path

from src.log import LogHandler
from src.data.setting import PROCESSED_DATA_DIR, DATE_PATTERN, CODE2NAME_PATH
from src.data.future.setting import SPREAD_DIR
from src.data.future.spread import get_future_spreads
from src.data.util import convert_percent

log = LogHandler('future.log')

# 列的英文名称
columns = ['commodity', 'sprice', 'recent_code', 'recent_price', 'recent_basis', 'recent_basis_prt', 'dominant_code',
           'dominant_price', 'dominant_basis', 'dominant_basis_prt', 'datetime', 'exchange']


def construct_spreads(start=datetime(2011, 1, 2), end=None):
    """
    hdf5 文件知道如何插入记录，暂时只能添加在文件尾部，因此要保证 历史数据连续
    :param start:
    :param end:
    :return:
    """
    end = datetime.today() if end is None else end

    target = PROCESSED_DATA_DIR / 'spread.h5'
    source = SPREAD_DIR
    update = None

    if target.exists():
        # 找最后一个记录的时间，所有商品相同
        last = pd.read_hdf(target, 'table', start=-1)
        update = last.index[0] + timedelta(1)

        if update > end:
            return

    # 保证每次将数据更新需要的程度, 数据虽然不需要更新，但是hdf文件不需要更新，不用判断
    get_future_spreads(start=update, end=end)

    # concat raw data from specific date
    file_df = pd.DataFrame([(pd.to_datetime([re.search(DATE_PATTERN, x.name)[0]]), x)
                            for x in source.glob('*.csv')], columns=['datetime', 'filepath'])
    file_df.set_index('datetime', inplace=True)
    if update:
        file_df.query("index>=Timestamp('{}') & index<=Timestamp('{}')".format(update, end),
                      inplace=True)

    if file_df.empty:
        return

    dtype = {'现货价格': 'float64', '最近合约价格': 'float64', '最近合约现期差1': 'float64',
             '主力合约价格': 'float64', '主力合约现期差2': 'float64', "最近合约代码": 'object',
             "主力合约代码": 'object'}

    frames = [pd.read_csv(x.filepath, encoding='gb2312', header=0, index_col=False,
                          parse_dates=['日期'], dtype=dtype)
              for x in file_df.itertuples()]

    if len(frames) == 0:
        return
    spread_df = pd.concat(frames, ignore_index=True)

    code2name_df = pd.read_csv(CODE2NAME_PATH, encoding='gb2312', header=0,
                               usecols=['code', 'spread']).dropna()
    code2name_df.set_index('spread', inplace=True)
    spread_df = spread_df.join(code2name_df, on='商品')
    spread_df['最近合约期现差百分比1'] = spread_df['最近合约期现差百分比1'].apply(convert_percent)
    spread_df['主力合约现期差百分比2'] = spread_df['主力合约现期差百分比2'].apply(convert_percent)
    spread_df.set_index('日期', inplace=True)
    try:
        spread_df.to_hdf(target, 'table', format='table',
                         append=True, complevel=5, complib='blosc')
    except ValueError:  # TypeError
        log.warning('{}'.format(spread_df.columns))


def get_spreads(commodity=None, start=None, end=None):
    """
    :param commodity  商品简称
    :param start
    :param end:
    :return pd.DataFrame
    """
    if end is None:
        end = datetime.today()

    construct_spreads(start=start, end=end)

    target = PROCESSED_DATA_DIR / 'spread.h5'

    if start and end:
        filtering = "index>=Timestamp('{}') & index<=Timestamp('{}')".format(start, end)
    elif start:
        filtering = "index>=Timestamp('{}')".format(start)
    elif end:
        filtering = "index<=Timestamp('{}')".format(end)
    else:
        filtering = ''

    if filtering:
        spread_df = pd.read_hdf(target, 'table', where=filtering)
    else:
        spread_df = pd.read_hdf(target, 'table')

    if commodity is None:
        return spread_df
    else:
        return spread_df[spread_df['code'] == commodity]


if __name__ == '__main__':
    # end_dt = datetime.today()
    # from src.data.future.inventory import get_future_inventory
    start_dt = datetime(2018, 12, 21)
    end_dt = datetime(2019, 3, 31)
    print(datetime.now())
    # get_future_inventory(start=datetime(2014, 5, 23), end=datetime.today())
    df = get_spreads('M', start=start_dt, end=None)
    print(datetime.now())

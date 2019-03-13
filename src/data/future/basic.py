# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from src.log import LogHandler
from src.data.setting import raw_data_dir, processed_data_dir
from src.data.future.spread import get_future_spreads, columns

log = LogHandler('future.basic.log')


def construct_spread_by_commodity(start=datetime(2011, 1, 2), end=datetime.today()):
    """
    hdf5 文件知道如何插入记录，暂时只能添加在文件尾部，因此要保证 历史数据连续
    :param start:
    :param end:
    :return:
    """
    target = processed_data_dir / 'spread.h5'
    source = raw_data_dir / 'spread'

    # 保证每次将数据更新到最新
    get_future_spreads(start=start, end=end)

    # concat raw data from specific date
    file_df = pd.DataFrame([(pd.to_datetime(x.name[:-4]), str(x)) for x in source.glob('*.csv')],
                           columns=['datetime', 'filepath'])
    file_df.set_index('datetime', inplace=True)
    file_df = file_df[file_df.index >= start]
    spread_df = pd.DataFrame()

    for file_name in file_df.values:
        df = pd.read_csv(str(file_name), enconding='gb2312')
        if not df.empty:
            spread_df.append(df)

    spread_df.to_hdf(target, 'table', format='table',
                     append=True, complevel=5, complib='blosc')


def get_spreads(commodity=None, start=None, end=None):
    """
    :param commodity  商品简称
    :param start
    :param end:
    :return pd.DataFrame
    """
    file_path = processed_data_dir / 'spread.h5'
    file_string = str(file_path)

    if file_path.exists():
        # 找最后一个记录的时间，所有商品相同
        last = pd.read_hdf(file_string, 'table', start=-1)
        update = list(last.index)[0]

        if update < start:
            construct_spread_by_commodity(update + timedelta(1), end)
    else:
        construct_spread_by_commodity(end=end)

    if start and end:
        filtering = "index>=Timestamp('{}') & index<=Timestamp('{}')".format(start, end)
    elif start:
        filtering = "index>=Timestamp('{}')".format(start)
    elif end:
        filtering = "index<=Timestamp('{}')".format(end)
    else:
        filtering = ''

    if filtering:
        df = pd.read_hdf(file_string, 'table', where=filtering)
    else:
        df = pd.read_hdf(file_string, 'table')

    return df


if __name__ == '__main__':
    # end_dt = datetime.today()
    start_dt = datetime(2011, 1, 2)
    end_dt = datetime(2011, 1, 7)
    print(datetime.now())
    get_spreads(start=start_dt, end=end_dt)
    print(datetime.now())

# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime, timedelta

from pathlib import Path

from src.log import LogHandler
from src.data.setting import raw_data_dir, processed_data_dir
from src.data.future.spread import get_future_spreads
from src.data.util import convert_percent

log = LogHandler('future.log')

# 列的英文名称
columns = ['commodity', 'sprice', 'recent_code', 'recent_price', 'recent_basis', 'recent_basis_prt', 'dominant_code',
           'dominant_price', 'dominant_basis', 'dominant_basis_prt', 'datetime', 'exchange']


def construct_spread_by_commodity(start=datetime(2011, 1, 2), end=datetime.today()):
    """
    hdf5 文件知道如何插入记录，暂时只能添加在文件尾部，因此要保证 历史数据连续
    :param start:
    :param end:
    :return:
    """
    target = processed_data_dir / 'spread.h5'
    source = raw_data_dir / 'spread'

    # 保证每次将数据更新需要的程度
    if get_future_spreads(start=start, end=end) is False:
        return

    # concat raw data from specific date
    file_df = pd.DataFrame([(pd.to_datetime(x.name[:-4]), str(x))
                            for x in source.glob('*.csv')], columns=['datetime', 'filepath'])
    file_df.set_index('datetime', inplace=True)
    file_df.query("index>=Timestamp('{}') & index<=Timestamp('{}')".format(start, end),
                  inplace=True)

    if file_df.empty:
        return

    frames = [pd.read_csv(x.filepath, encoding='gb2312', header=0, index_col=False, parse_dates=['日期'],
                          dtype={'现货价格': 'float64', '最近合约价格': 'float64', '最近合约现期差1': 'float64',
                                 '主力合约价格': 'float64', '主力合约现期差2': 'float64', "最近合约代码": 'object',
                                 "主力合约代码": 'object'})
              for x in file_df.itertuples()]

    if frames:
        return
    spread_df = pd.concat(frames, ignore_index=True)

    file_path = Path(__file__).parent / 'code2name.csv'
    code2name_df = pd.read_csv(str(file_path), encoding='gb2312', header=0, usecols=['code', 'spread']).dropna()
    code2name_df.set_index('spread', inplace=True)
    spread_df = spread_df.join(code2name_df, on='商品')
    spread_df['最近合约期现差百分比1'] = spread_df['最近合约期现差百分比1'].apply(convert_percent)
    spread_df['主力合约现期差百分比2'] = spread_df['主力合约现期差百分比2'].apply(convert_percent)
    # spread_df['日期'] = pd.to_datetime(spread_df['日期'])
    spread_df.set_index('日期', inplace=True)
    try:
        spread_df.to_hdf(str(target), 'table', format='table',
                         append=True, complevel=5, complib='blosc')
    except ValueError:  # TypeError
        log.warning('{}'.format(spread_df.columns))


def get_spreads(commodity=None, start=None, end=datetime.today()):
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
        update = last.index[0] + timedelta(1)

        if update < end:
            construct_spread_by_commodity(update, end)
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
        spread_df = pd.read_hdf(file_string, 'table', where=filtering)
    else:
        spread_df = pd.read_hdf(file_string, 'table')

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

# --coding:utf-8
"""
block
~~~~~~~~~~~~
This module contains the identification of extremes, segments and blocks.
"""
from log import LogHandler
# from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import scipy.signal as signal

# TODO 改用动态接口
# get_history_hq_api(code, start=None, end=None, freq='d')
from tdx import get_future_hq

get_history_hq_api = get_future_hq

FREQ = ('5m', '30m', 'h', 'd', 'w', 'm', 'q')

log = LogHandler('block')


class TsBlock:

    def __init__(self, code):
        self.code = code
        self.__extremes = dict.fromkeys(FREQ)
        self.__segments = dict.fromkeys(FREQ)
        self.__blocks = dict.fromkeys(FREQ, pd.DataFrame())

    def get_extremes(self, start=None, end=None, freq='d'):
        """
        only append the data at the end,  ascending sort
        :param start: datetime
        :param end: datetime
        :param freq: ('5m', '30m', 'd', 'w')
        :return: [pd.Series, pd.Series]
        """

        temp_extreme = self.__extremes[freq]

        if temp_extreme:
            if start and end:
                return [temp_extreme[0][start:end], temp_extreme[1][start:end]]
            elif start:
                return [temp_extreme[0][start:], temp_extreme[1][start:]]
            elif end:
                return [temp_extreme[0][:end], temp_extreme[1][:end]]
            else:
                return temp_extreme
        elif freq not in ('5m', 'd'):
            index = FREQ.index(freq) - 1
            high, low = self.get_segments(start=start, end=end, freq=FREQ[index])
        else:
            temp_df = get_history_hq(self.code, start=start, end=end, freq=freq)
            high = temp_df.high
            low = temp_df.low

        temp_extreme = get_kline_extremes(high, low)
        self.__extremes[freq] = temp_extreme

        return temp_extreme

    def get_segments(self, start=None, end=None, freq='d'):
        """
                only append the data at the end,  ascending sort
                :param start: datetime
                :param end: datetime
                :param freq: ('5m', '30m', 'd', 'w')
                :return: [pd.Series, pd.Series]
                """
        temp_segment = self.__segments[freq]

        if temp_segment:
            if start and end:
                return [temp_segment[0][start:end], temp_segment[1][start:end]]
            elif start:
                return [temp_segment[0][start:], temp_segment[1][start:]]
            elif end:
                return [temp_segment[0][:end], temp_segment[1][:end]]
            else:
                return temp_segment
        else:
            higher, lower = self.get_extremes(start=start, end=end, freq=freq)
            temp_segment = get_kline_segments(higher, lower)
            self.__segments[freq] = temp_segment
            return temp_segment

    def get_blocks(self, start=None, end=None, freq='d'):
        """
        only append the data at the end,  ascending sort
        :param start: datetime
        :param end: datetime
        :param freq: ('5m', '30m', 'd', 'w')
        :return: pd.Dataframe   columns=['end_dt', 'block_high', 'block_low', 'block_highest', 'block_lowest',
                                         'segment_num', 'block_flag', 'block_hl_flag', 'top_bottom_flag']
        """

        temp_block = self.__blocks[freq]

        if temp_block.empty:
            higher, lower = self.get_segments(start=start, end=end, freq=freq)
            temp_block = identify_blocks(higher, lower)
            temp_block = identify_blocks_relation(temp_block)
            self.__blocks[freq] = temp_block
            return temp_block
        # TODO 增加读取和写入存盘文件操作
        else:
            if start and end:
                return temp_block[start:end]
            elif start:
                return temp_block[start:]
            elif end:
                return temp_block[0]
            else:
                return temp_block

    def get_current_status(self, start=None, end=None, freq='d'):
        temp_block_df = self.get_blocks(start=None, end=None, freq=freq)
        dt = temp_block_df.loc[temp_block_df['No.'] == 0, ['block_flag', 'No.', 'segment_num', 'block_hl_flag']].tail(
            2).index[0]
        return temp_block_df.loc[dt:]


def identify_blocks_relation(block_df):
    block_relation_df = block_df[block_df['segment_num'] > 3].diff()[1:]
    temp_df = block_df.copy(deep=True)
    temp_df['block_flag'] = '-'
    temp_df['block_hl_flag'] = '-'
    temp_df['No.'] = '_'
    # temp_df['No_include_3'] = '_'
    # temp_df['top_bottom_flag'] = '-'

    block_index = 0
    # block_index_incl3 = 0

    for row in block_relation_df.itertuples():
        current_dt = row.Index
        # prev_index = block_relation_df.index.get_loc(current_dt) - 1

        if row.block_high > 0 and row.block_low > 0:
            block_flag = 'up'
        elif row.block_high < 0 and row.block_low < 0:
            block_flag = 'down'
        elif row.block_high > 0 > row.block_low:
            block_flag = 'include'
        elif row.block_high < 0 < row.block_low:
            block_flag = 'included'

        if row.block_highest > 0 and row.block_lowest > 0:
            block_hl_flag = 'up'
        elif row.block_highest < 0 and row.block_lowest < 0:
            block_hl_flag = 'down'
        elif row.block_highest > 0 > row.block_lowest:
            block_hl_flag = 'include'
        elif row.block_highest < 0 < row.block_lowest:
            block_hl_flag = 'included'

        temp_df.loc[current_dt, 'block_flag'] = block_flag
        temp_df.loc[current_dt, 'block_hl_flag'] = block_hl_flag

        block_index = block_index + 1

        # 最后一个block不能确认是top或者bottom,segment_num < 4的情况要计算在内
        if temp_df.segment_num[current_dt] % 2 == 0 and current_dt != block_df.index[-1]:
            if block_flag == 'up':
                temp_df.loc[current_dt, 'block_flag'] = 'top'
            elif block_flag == 'down':
                temp_df.loc[current_dt, 'block_flag'] = 'bottom'
            block_index = 0

        temp_df.loc[current_dt, 'No.'] = block_index

    return temp_df


def identify_blocks(higher, lower):
    # 后向寻找Block
    gd_df = pd.concat([higher, lower], axis=1, join='outer')
    hl_df = gd_df.fillna(0)

    # init current block
    block_high = higher[0]
    block_low = lower[0]
    start_dt = hl_df.index[0]
    end_dt = hl_df.index[1]
    segment_num = 1
    current_dt = start_dt
    # 初始化block表
    block_df = pd.DataFrame(
        columns=['start_dt', 'end_dt', 'block_high', 'block_low', 'block_highest', 'block_lowest',
                 'segment_num'])

    for row in hl_df[2:].itertuples():
        # print(row.Index)
        # print([current_dt, start_dt, end_dt, block_high,block_low,block_highest, block_lowest,segment_num])
        if segment_num < 2:  # 一上一下2根线段必定有交集,不需要判断是否是新的block
            current_dt = row.Index
            segment_num = segment_num + 1
            if row.high > row.low:  # 顶
                block_high = min(block_high, row.high)
            else:
                block_low = max(block_low, row.low)
        else:
            if row.high > row.low:  # 顶
                if row.high < block_low:  # 第三类卖点，新的中枢开始
                    start_index = gd_df.index.get_loc(start_dt) + 1
                    end_index = gd_df.index.get_loc(current_dt)
                    block_highest = gd_df.high[start_index: end_index].max()
                    block_lowest = gd_df.low[start_index: end_index].min()

                    insert_row = pd.DataFrame([[start_dt, current_dt, block_high, block_low, block_highest,
                                                block_lowest, segment_num]],
                                              columns=['start_dt', 'end_dt', 'block_high', 'block_low',
                                                       'block_highest', 'block_lowest', 'segment_num'])
                    block_df = block_df.append(insert_row, ignore_index=True)

                    start_dt = end_dt
                    segment_num = 2
                    block_high = row.high
                    block_low = lower[current_dt]
                else:
                    segment_num = segment_num + 1
                    block_high = min(block_high, row.high)
            else:
                if row.low > block_high:  # 第三类买点，新的中枢开始
                    start_index = gd_df.index.get_loc(start_dt) + 1
                    end_index = gd_df.index.get_loc(current_dt)
                    block_highest = gd_df.high[start_index: end_index].max()
                    block_lowest = gd_df.low[start_index: end_index].min()

                    insert_row = pd.DataFrame([[start_dt, current_dt, block_high, block_low, block_highest,
                                                block_lowest, segment_num]],
                                              columns=['start_dt', 'end_dt', 'block_high', 'block_low',
                                                       'block_highest', 'block_lowest', 'segment_num'])
                    block_df = block_df.append(insert_row, ignore_index=True)

                    start_dt = end_dt
                    segment_num = 2
                    block_low = row.low
                    block_high = higher[current_dt]
                else:
                    segment_num = segment_num + 1
                    block_low = max(block_low, row.low)
            end_dt = current_dt
            current_dt = row.Index

    # record last block
    start_index = gd_df.index.get_loc(start_dt) + 1
    end_index = gd_df.index.get_loc(current_dt)
    block_highest = gd_df.high[start_index: end_index].max()
    block_lowest = gd_df.low[start_index: end_index].min()

    insert_row = pd.DataFrame([[start_dt, current_dt, block_high, block_low, block_highest,
                                block_lowest, segment_num]],
                              columns=['start_dt', 'end_dt', 'block_high', 'block_low',
                                       'block_highest', 'block_lowest', 'segment_num'])
    block_df = block_df.append(insert_row, ignore_index=True)
    return block_df.set_index('start_dt')


def get_kline_segments(higher, lower):
    """
    identify the segments from high/low extremes
    :param higher: pd.Series, index is datetime
    :param lower:  pd.Series, index is datetime
    :return: [pd.Series, pd.Series]
    """
    hl_df = pd.concat([higher, lower], axis=1, join='outer')
    # 比较前后高低点
    df1 = hl_df.diff()
    df2 = hl_df.diff(-1)

    # TODO   需要删除相隔较近的高低点
    # 需要删除的高低点，连续高低点中的较低高点和较高低点,相等的情况，删除后面的点
    index = [df1['high'] <= 0, df2['high'] < 0, df1['low'] >= 0, df2['low'] > 0]
    flag = [x.any() for x in index]

    if not (flag[0] or flag[1] or flag[2] or flag[3]):
        return [higher, lower]

    # 处理连续的高低点中，高点比低点低的情况

    # 删除无效的高低点
    if flag[0]:
        hl_df.loc[index[0], 'high'] = np.nan  # 向后删除较低高点
    if flag[1]:
        hl_df.loc[index[1], 'high'] = np.nan  # 向前删除较低高点
    if flag[2]:
        hl_df.loc[index[2], 'low'] = np.nan
    if flag[3]:
        hl_df.loc[index[3], 'low'] = np.nan
    if flag[0] or flag[1]:
        higher = hl_df['high'].dropna()
    if flag[2] or flag[3]:
        lower = hl_df['low'].dropna()

    # 合并高低点后再处理一次
    return get_kline_segments(higher, lower)


def get_kline_extremes(high, low):
    """
    calculate the get_kline_extremes values of high and low
    :param high: pd.Series, index is datetime
    :param low:  pd.Series, index is datetime
    :return: [pd.Series, pd.Series]
    """
    higher = high.iloc[signal.argrelextrema(high.values, np.greater)]
    lower = low.iloc[signal.argrelextrema(-low.values, np.greater)]

    return [higher, lower]


def get_history_hq(code, start=None, end=None, freq='d'):
    """
    get history bar from external api
    :param code:
    :param start: datetime
    :param end: datetime
    :param freq: '5m', '30m', 'd', 'w'
    :return: pd.Dataframe columns=['high', 'low']
    """
    if get_history_hq_api:
        return get_history_hq_api(code=code, start=start, end=end, freq=freq)
    else:  # only for test
        from pytdx.reader import TdxExHqDailyBarReader
        reader = TdxExHqDailyBarReader()
        temp_df = reader.get_df(r"D:\Trade\TDX\vipdoc\ds\lday\28#SRL9.day")

        return temp_df.loc[:, ['high', 'low']]


if __name__ == "__main__":
    from datetime import datetime, timedelta
    from block import TsBlock
    import pandas as pd

    today = datetime.today()
    observation = 365
    start = today - timedelta(observation)
    end = today - timedelta(7)

    block = TsBlock("SRL9")
    segment = block.get_segments()
    df = pd.concat(segment, axis=1, join='outer').fillna(0)
    block_df = block.get_blocks()

    # TODO external api 如何使用

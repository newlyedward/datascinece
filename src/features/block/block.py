# --coding:utf-8
"""
block
~~~~~~~~~~~~
This module contains the identification of peaks, segments and blocks.
"""
from src.log import LogHandler
# from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import scipy.signal as signal

# TODO 改用动态接口
# get_history_hq_api(code, start=None, end=None, freq='d')
from src.data.tdx import get_future_hq

get_history_hq_api = get_future_hq

FREQ = ('5m', '30m', 'h', 'd', 'w', 'm', 'q')

log = LogHandler('block.log')


class TsBlock:

    def __init__(self, code):
        self.code = code
        self.__peaks = dict.fromkeys(FREQ, pd.DataFrame())
        self.__segments = dict.fromkeys(FREQ, pd.DataFrame())
        self.__blocks = dict.fromkeys(FREQ, pd.DataFrame())

    def get_peaks(self, start=None, end=None, freq='d'):
        """
        only append the data at the end,  ascending sort
        :param start: datetime
        :param end: datetime
        :param freq: ('5m', '30m', 'd', 'w')
        :return: [pd.DataFrame/Series, pd.DataFrame/Series]  升级形成的peak不带kline_no.
        """

        temp_peak = self.__peaks[freq]

        if temp_peak.empty:
            if freq in ('5m', 'd'):
                temp_df = get_history_hq(self.code, start=start, end=end, freq=freq)
                hq_df = temp_df[['high', 'low', 'kindex']]
                temp_peak = get_peaks_from_hq(hq_df)
            else:
                index = FREQ.index(freq) - 1
                peak_df = self.get_segments(start=start, end=end, freq=FREQ[index])
                temp_peak = get_peaks_from_segments(peak_df)
            self.__peaks[freq] = temp_peak
            return temp_peak
        elif start and end:
            return temp_peak[start:end]
        elif start:
            return temp_peak[start:]
        elif end:
            return temp_peak[:end]
        else:
            return temp_peak

    def get_segments(self, start=None, end=None, freq='d'):
        """

        :param start: datetime
        :param end: datetime
        :param freq: ('5m', '30m', 'd', 'w')
        :return: [pd.Series, pd.Series]
        """
        segment_df = self.__segments[freq]

        if segment_df.empty:
            peak_df = self.get_peaks(start=start, end=end, freq=freq)
            segment_df = get_segments(peak_df)
            self.__segments[freq] = segment_df
            return segment_df
        else:
            if start and end:
                return segment_df[start:end]
            elif start:
                return segment_df[start:]
            elif end:
                return segment_df[:end]
            else:
                return segment_df

    def get_blocks(self, start=None, end=None, freq='d'):
        """
        only append the data at the end,  ascending sort
        :param start: datetime
        :param end: datetime
        :param freq: ('5m', '30m', 'd', 'w')
        :return: pd.Dataframe   columns=['end_dt', 'block_high', 'block_low', 'block_highest', 'block_lowest',
                                         'segment_num', 'block_flag', 'block_type', 'top_bottom_flag']
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
        dt = temp_block_df.loc[temp_block_df['No.'] == 0, ['block_flag', 'No.', 'segment_num', 'block_type']].tail(
            2).index[0]
        return temp_block_df.loc[dt:]


def identify_blocks_relation(block_df):
    block_relation_df = block_df[block_df['segment_num'] > 3].diff()[1:]
    temp_df = block_df.copy(deep=True)
    temp_df['block_flag'] = '-'
    temp_df['block_type'] = '-'
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
            block_type = 'up'
        elif row.block_highest < 0 and row.block_lowest < 0:
            block_type = 'down'
        elif row.block_highest > 0 > row.block_lowest:
            block_type = 'include'
        elif row.block_highest < 0 < row.block_lowest:
            block_type = 'included'

        temp_df.loc[current_dt, 'block_flag'] = block_flag
        temp_df.loc[current_dt, 'block_type'] = block_type

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


# TODO 增加对 删除连续高低点；删除低点比高点高的段；删除高低点较近的段 的测试用例


def sort_one_candle_peaks(peak_df, invert=True):
    """
    对同一个k线的极值点进行排序
    :param peak_df: pd.DataFrame columns=[peak, kindex, type]
    :param invert: default True, 按 high， high和low， low排列
    :return: pd.DataFrame columns=[high, peak, type]
    """
    # 检测输入类型
    assert isinstance(peak_df, pd.DataFrame)

    df = peak_df.copy()  # 不对输入数据进行操作，有可能改变原始数据
    df['sn'] = range(len(df))

    # TODO 不存在同一个k线上有极值点的处理
    for row in df[df.index.duplicated()].itertuples():
        try:
            if df.type[row.sn] != df.type[row.sn + 1] and invert:
                df.iloc[row.sn], df.iloc[row.sn - 1] = df.iloc[row.sn - 1], df.iloc[row.sn]
        except IndexError:
            log.info('最后一根k线上有极值点')

    # df.to_excel('one.xlsx')
    del df['sn']
    return df


def remove_fake_peaks(peak_df):
    """
    删除比相邻极点高的低点和比相邻极点低的高点，包括了高点/低点连续出现的情况
    :param peak_df: pd.DataFame, columns=[peak, kindex, type]
    :return: pd.DataFame, columns=[peak, kindex, type]
    """

    # 检测输入类型
    assert isinstance(peak_df, pd.DataFrame)

    df = peak_df.copy()  # 不对输入数据进行操作，有可能改变原始数据
    # df['sn'] = range(len(df))
    len_before = len(df)
    len_after = len_before - 1

    while len_after < len_before:
        print(len_after)

        diff_left = df.peak.diff()
        diff_right = df.peak.diff(-1)

        # 和前一个极值点点比较，没有比较的情况假设是合理的极值点
        diff_left[0] = -1 if df.type[0] == 'low' else 1
        diff_right[-1] = -1 if df.type[-1] == 'low' else 1

        h_flag = np.logical_and(np.logical_and(diff_left >= 0, diff_right > 0),
                                df.type == 'high')
        r_flag = np.logical_and(np.logical_and(diff_left <= 0, diff_right < 0),
                                df.type == 'low')

        # # 相等的情况保留后一个点
        flag = np.logical_or(h_flag, r_flag)

        df = df[flag.values]

        len_before = len_after
        len_after = len(df)

    # df.to_excel('fake_.xlsx')

    return df


def get_segments(peak_df):
    """
    identify the segments from peaks
    :param peak_df: pd.DataFame, columns=[peak, kindex, type]
    :return: pd.DataFame, columns=[peak, kindex, type]
    """
    df = peak_df.copy()

    # len_before = len(df)
    # len_after = len_before - 1
    # # 删除连续高高或者低低点中的较低高点和较高低点,相等的情况，删除后面的点
    # while len_after < len_before:
    #     print(len_after)
    #     df = remove_fake_peaks(df)
    #     len_before = len_after
    #     len_after = len(df)

    df = remove_fake_peaks(df)
    df = sort_one_candle_peaks(df)
    df = remove_fake_peaks(df)

    df.to_excel('segment_ww.xlsx')
    # TODO 极值点离得较近的情况

    return df


def get_peaks_from_segments(segment_df):
    """
    only calculate the peaks of kline value of high and low
    :param segment_df: pd.DataFame,
            columns=['peak', 'kindex', 'type']
    :return: pd.DataFame, columns=[peak, kindex, type]
        peak        high,low的极值点
        kindex      k线的序列号
        type        'high' or 'low'
    """

    high_df = segment_df[segment_df['type'] == 'high']
    low_df = segment_df[segment_df['type'] == 'low']

    # 值相等的极值点全部保留
    high_df = high_df.iloc[signal.argrelextrema(high_df.peak.values, np.greater_equal)]
    low_df = low_df.iloc[signal.argrelextrema(-low_df.peak.values, np.greater_equal)]

    peak_df = high_df.append(low_df).sort_index()

    # 添加被错误删除的点，即两个端点之间还有更高的高点和更低的低点
    x = peak_df.reindex(segment_df.index, method='bfill')
    y = peak_df.reindex(segment_df.index, method='ffill')

    # TODO bpeak不可以，下面单独出来独立为函数
    bpeak = x.type == segment_df.type
    b_compare = x.peak - segment_df.peak
    f_compare = y.peak - segment_df.peak
    b1 = np.logical_and(b_compare > 0, x.type == 'low')
    b2 = np.logical_and(b_compare < 0, x.type == 'high')
    b3 = np.logical_and(f_compare > 0, y.type == 'low')
    b4 = np.logical_and(f_compare < 0, y.type == 'high')
    bflag = np.logical_and(bpeak, np.logical_or(np.logical_or(b1, b2),
                                               np.logical_or(b3, b4)))

    # peak_df.to_excel('peak_ww.xlsx')

    return peak_df


def get_peaks_from_hq(hq_df):
    """
    only calculate the peaks of kline value of high and low
    :param hq_df: pd.DataFame,
            columns=['high', 'low', 'kindex']
    :return: pd.DataFame, columns=[peak, kindex, type]
        peak        high,low的极值点
        kindex      k线的序列号
        type        'high' or 'low'
    """
    high_df = hq_df.rename(columns={'high': 'peak'})
    del high_df['low']
    high_df['type'] = 'high'

    # low_df = lower.low.to_frame('peak')
    low_df = hq_df.rename(columns={'low': 'peak'})
    del low_df['high']
    low_df['type'] = 'low'

    # 值相等的极值点全部保留
    high_df = high_df.iloc[signal.argrelextrema(high_df.peak.values, np.greater_equal)]
    low_df = low_df.iloc[signal.argrelextrema(-low_df.peak.values, np.greater_equal)]

    peak_df = high_df.append(low_df).sort_index()

    # peak_df.to_excel('peak_ww.xlsx')

    return peak_df


def get_history_hq(code, start=None, end=None, freq='d'):
    """
    get history bar from external api
    :param code:
    :param start: datetime
    :param end: datetime
    :param freq: '5m', '30m', 'd', 'w'
    :return: pd.Dataframe columns=['high', 'low', 'serial_number]
    """
    if get_history_hq_api:
        temp_hq_df = get_history_hq_api(code=code, start=start, end=end, freq=freq)
        temp_hq_df['kindex'] = range(len(temp_hq_df))
        return temp_hq_df[['high', 'low', 'kindex']]
    else:  # only for test
        return None


if __name__ == "__main__":
    from datetime import datetime, timedelta

    # from src.features.block import TsBlock

    start = datetime(2018, 6, 29)
    end = datetime(2019, 3, 1)
    # observation = 250
    # start = today - timedelta(observation)
    # end = today - timedelta(7)

    block = TsBlock("ML8")
    # peak = block.get_peaks(start=start, end=end)
    # segment = block.get_segments(start=start)
    segment = block.get_segments(freq='w')
    # df = pd.concat(segment, axis=1, join='outer').fillna(0)
    # block_df = block.get_blocks()

    # TODO external api 如何使用
    # ml8 2018 9-10month  发现错误的段，低点比高点高·······························

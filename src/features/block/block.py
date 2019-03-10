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
            segment_df = get_segments_from_peaks(peak_df)
            self.__segments[freq] = segment_df
            segment_df.to_excel("segment_" + freq + ".xlsx")
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
            segment_df = self.get_segments(start=start, end=end, freq=freq)
            temp_block = identify_blocks(segment_df)
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
        temp_block_df = self.get_blocks(start=start, end=start, freq=freq)
        try:
            dt = temp_block_df.loc[temp_block_df['No.'] == 0].tail(2).index[0]
            return temp_block_df.loc[dt:]
        except IndexError:
            return temp_block_df


def identify_blocks_relation(df: pd.DataFrame):
    """

    :param df: pd.DataFrame(columns=['enter_dt', 'start_dt', 'end_dt', 'left_dt', 'block_high', 'block_low',
                               'block_highest', 'block_lowest', 'segment_num'])
    :return: pd.DataFrame(columns=['enter_dt', 'start_dt', 'end_dt', 'left_dt', 'block_high', 'block_low',
                               'block_highest', 'block_lowest', 'segment_num', 'block_type', 'block_type', 'No.'])
        block_type: up, down
        block_relation: up, down, overlap, include, included
        No. 对趋势中的block进行计数，只有三段的不计数
    """

    # block_relation_df = df[df['segment_num'] > 3].diff()[1:]
    temp_df = df.copy()
    temp_df['block_type'] = np.nan
    temp_df['block_relation'] = np.nan
    temp_df['No.'] = np.nan

    prev_high = temp_df.block_high[0]
    prev_highest = temp_df.block_highest[0]
    prev_low = temp_df.block_low[0]
    prev_lowest = temp_df.block_lowest[0]

    block_index = 0
    block_type = block_relation = np.nan

    for row in temp_df[1:].itertuples():
        current_dt = row.Index
        segment_num = row.segment_num
        current_high = row.block_high
        current_highest = row.block_highest
        current_low = row.block_low
        current_lowest = row.block_lowest

        if current_low > prev_high:
            block_type = 'up'
        elif current_high < prev_low:
            block_type = 'down'
        else:
            log.info('Wrong identification of block!')

        if current_highest >= prev_highest and current_lowest >= prev_lowest:
            if prev_highest > current_lowest:
                block_relation = 'overlap'
            else:
                block_relation = 'up'
        elif current_highest <= prev_highest and current_lowest <= prev_lowest:
            if current_highest > prev_lowest:
                block_relation = 'overlap'
            else:
                block_relation = 'down'
        elif current_highest >= prev_highest and current_lowest <= prev_lowest:
            block_relation = 'include'
        elif current_highest <= prev_highest and current_lowest >= prev_lowest:
            block_relation = 'included'
        else:
            log.info('Wrong identification of block!')

        temp_df.loc[current_dt, 'block_type'] = block_type
        temp_df.loc[current_dt, 'block_relation'] = block_relation

        if segment_num > 3:
            block_index = block_index + 1

        # 最后一个block不能确认是top或者bottom,segment_num < 4的情况要计算在内
        if segment_num % 2 == 0 and current_dt != temp_df.index[-1]:
            # if block_type == 'up':
            #     temp_df.loc[current_dt, 'block_type'] = 'top'
            # elif block_type == 'down':
            #     temp_df.loc[current_dt, 'block_type'] = 'bottom'
            block_index = 0

        temp_df.loc[current_dt, 'No.'] = block_index
        prev_high = current_high
        prev_highest = current_highest
        prev_low = current_low
        prev_lowest = current_lowest

    return temp_df


def identify_blocks(segment_df: pd.DataFrame):
    """
    identify blocks
    :param segment_df: pd.DataFame, columns=[peak, kindex, type]
    :return: pd.DataFrame(columns=['enter_dt', 'start_dt', 'end_dt', 'left_dt', 'block_high', 'block_low',
                               'block_highest', 'block_lowest', 'segment_num'])
            enter_dt:
            start_dt:
            end_dt:
            left_dt:
            block_high:
            block_low:
            block_highest:
            block_lowest:
            segment_num:
    """
    # init current block
    block_high = block_highest = current_high = current_highest = segment_df.peak[1:3].max()
    block_low = block_lowest = current_low = current_lowest = segment_df.peak[1:3].min()

    enter_dt = segment_df.index[0]
    start_dt = segment_df.index[1]
    end_dt = left_dt = segment_df.index[2]
    segment_num = 2

    # 初始化block表
    columns = ['enter_dt', 'start_dt', 'end_dt', 'left_dt', 'block_high', 'block_low',
               'block_highest', 'block_lowest', 'segment_num']
    df = pd.DataFrame(columns=columns)

    for row in segment_df[3:].itertuples():
        current_dt = row.Index
        if row.type == 'high':  # 顶
            if row.peak < block_low:  # 第三类卖点，新的中枢开始
                insert_row = pd.DataFrame([[enter_dt, start_dt, end_dt, left_dt, block_high, block_low,
                                            block_highest, block_lowest, segment_num]], columns=columns)
                df = df.append(insert_row, ignore_index=True)

                enter_dt = end_dt
                start_dt = left_dt
                end_dt = left_dt = current_dt
                segment_num = 2
                block_high = block_highest = current_high = current_highest = row.peak
                block_low = block_lowest = current_low = current_lowest = segment_df.loc[start_dt, 'peak'].min() \
                    if isinstance(segment_df.loc[start_dt, 'peak'], pd.Series) else segment_df.loc[start_dt, 'peak']
            else:
                segment_num = segment_num + 1
                block_low = current_low
                current_high = min(block_high, row.peak)
                block_lowest = current_lowest
                current_highest = max(block_highest, row.peak)
                end_dt = left_dt
                left_dt = current_dt
        else:
            if row.peak > block_high:  # 第三类买点，新的中枢开始
                insert_row = pd.DataFrame([[enter_dt, start_dt, end_dt, left_dt, block_high, block_low,
                                            block_highest, block_lowest, segment_num]], columns=columns)
                df = df.append(insert_row, ignore_index=True)

                enter_dt = end_dt
                start_dt = left_dt
                end_dt = left_dt = current_dt
                segment_num = 2
                block_high = block_highest = current_high = current_highest = segment_df.loc[start_dt, 'peak'].max() \
                    if isinstance(segment_df.loc[start_dt, 'peak'], pd.Series) else segment_df.loc[start_dt, 'peak']
                block_low = block_lowest = current_low = current_lowest = row.peak
            else:
                segment_num = segment_num + 1
                block_high = current_high
                current_low = max(block_low, row.peak)
                block_highest = current_highest
                current_lowest = min(block_lowest, row.peak)

                end_dt = left_dt
                left_dt = current_dt

    # record last block
    insert_row = pd.DataFrame([[enter_dt, start_dt, end_dt, left_dt, block_high, block_low, block_highest,
                                block_lowest, segment_num]], columns=columns)
    df = df.append(insert_row, ignore_index=True)
    return df.set_index('enter_dt')


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
    len_after = len_before - 1  # 为了循环能执行至少一次

    while len_after < len_before:
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

    return df


def get_segments_from_peaks(peak_df):
    """
    identify the segments from peaks
    :param peak_df: pd.DataFame, columns=[peak, kindex, type]
    :return: pd.DataFame, columns=[peak, kindex, type]
    """
    df = peak_df.copy()

    df = remove_fake_peaks(df)
    df = sort_one_candle_peaks(df)
    df = remove_fake_peaks(df)

    # df.to_excel('segment_ww.xlsx')
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
    # todo segment_df.index SRL8中有重复项，不能用reindex
    x = peak_df.reindex(segment_df.index, method='bfill')
    y = peak_df.reindex(segment_df.index, method='ffill')

    # 修正端点不是极值点的错误
    b_compare = x.peak - segment_df.peak
    f_compare = y.peak - segment_df.peak
    b1 = np.logical_and(np.logical_and(b_compare > 0, x.type == 'low'), segment_df.type == 'low')
    b2 = np.logical_and(np.logical_and(b_compare < 0, x.type == 'high'), segment_df.type == 'high')
    b3 = np.logical_and(np.logical_and(f_compare > 0, y.type == 'low'), segment_df.type == 'low')
    b4 = np.logical_and(np.logical_and(f_compare < 0, y.type == 'high'), segment_df.type == 'high')
    bflag = np.logical_or(np.logical_or(b1, b2), np.logical_or(b3, b4))

    return peak_df.append(segment_df[bflag]).sort_index()


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
    # segment = block.get_segments(freq='w')
    # block_df = block.get_blocks()

    block_w = block.get_current_status(freq='w')
    # block_d = block.get_current_status(start=start)
    # segment_w = block.get_segments(start=start, freq='w')
    # segment_d = block.get_segments(start=start)

    # TODO external api 如何使用
    # ml8 2018 9-10month  发现错误的段，低点比高点高·······························

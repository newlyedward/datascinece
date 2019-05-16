# --coding:utf-8
"""
block
~~~~~~~~~~~~
This module contains the identification of peaks, segments and blocks.
"""
from bson.objectid import ObjectId
from pymongo import ASCENDING, DESCENDING

from log import LogHandler
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import scipy.signal as signal

# TODO 改用动态接口
# get_history_hq_api(code, start=None, end=None, freq='d')
from src.features import conn
from src.data.tdx import get_future_hq
from src.util import connect_mongo
from src.api.cons import FREQ

get_history_hq_api = get_future_hq

log = LogHandler('features.log')


class TsBlock:

    def __init__(self, code):
        self.code = code
        self.__peaks = dict.fromkeys(FREQ, pd.DataFrame())
        self.__segments = dict.fromkeys(FREQ, pd.DataFrame())
        self.__blocks = dict.fromkeys(FREQ, pd.DataFrame())
        # 从数据库读取数据

    def get_peaks(self, start=None, end=None, freq='d'):
        """
        only append the data at the end,  ascending sort
        :param start: datetime
        :param end: datetime
        :param freq: ('5m', '30m', 'd', 'w')
        :return: [pd.DataFrame/Series, pd.DataFrame/Series]
        """

        temp_peak = self.__peaks[freq]

        if temp_peak.empty:
            if freq in ('5m', 'd'):
                temp_df = get_history_hq(self.code, start_date=start, end_date=end, freq=freq)
                hq_df = temp_df[['high', 'low']]
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
        :return: pd.Dataframe   columns=['end_date', 'block_high', 'block_low', 'block_highest', 'block_lowest',
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
            dt = temp_block_df.loc[temp_block_df['sn'] == 0].tail(2).index[0]
            return temp_block_df.loc[dt:]
        except IndexError:
            return temp_block_df


def identify_blocks_relation(block_df: pd.DataFrame):
    """

    :param block_df: pd.DataFrame(columns=['enter_date', 'start_date', 'block_high', 'block_low',
                               'block_highest', 'block_lowest', 'segment_num'])
    :return: pd.DataFrame(columns=['enter_date', 'start_date', 'block_high', 'block_low',
                               'block_highest', 'block_lowest', 'segment_num', 'type', 'relation', 'sn'])
        block_type: up, down
        block_relation: up, down, overlap, include, included
        sn 对趋势中的block进行计数，只有三段的不计数
    """

    # block_relation_df = block_df[block_df['segment_num'] > 3].diff()[1:]
    temp_df = block_df.copy()

    if 'type' not in temp_df.columns:
        temp_df['type'] = np.nan
        temp_df['relation'] = np.nan
        temp_df['sn'] = 0

    type_index = temp_df.columns.get_loc('type')
    relation_index = temp_df.columns.get_loc('relation')
    sn_index = temp_df.columns.get_loc('sn')

    prev_high = temp_df.block_high[0]
    prev_highest = temp_df.block_highest[0]
    prev_low = temp_df.block_low[0]
    prev_lowest = temp_df.block_lowest[0]
    prev_segment_num = temp_df.segment_num[0]

    block_index = temp_df.iloc[0, sn_index]
    block_type = block_relation = np.nan

    last_index = len(temp_df) - 1

    for row in temp_df[1:].itertuples():
        index = row.Index
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

        temp_df.iloc[index, type_index] = block_type
        temp_df.iloc[index, relation_index] = block_relation

        if prev_segment_num > 3:  # segment_num = 3 的不认为是一个震荡区间
            block_index = block_index + 1

        # 最后一个block不能确认是top或者bottom,segment_num < 4的情况要计算在内
        if segment_num % 2 == 0 and index != last_index:
            block_index = 0

        temp_df.iloc[index, sn_index] = block_index
        prev_segment_num = segment_num
        prev_high = current_high
        prev_highest = current_highest
        prev_low = current_low
        prev_lowest = current_lowest

    return temp_df


def identify_blocks(segment_df: pd.DataFrame):
    """
    identify blocks
    :param segment_df: pd.DataFame, columns=[datetime, peak, type]
    :return: pd.DataFrame(columns=['enter_date', 'start_date', 'end_date', 'left_date', 'block_high', 'block_low',
                               'block_highest', 'block_lowest', 'segment_num'])
            enter_date:
            start_date:
            end_date:
            left_date:
            block_high:
            block_low:
            block_highest:
            block_lowest:
            segment_num:
    """
    # 按high，low间隔排列，防止同一天的极值点排列错误
    assert len(segment_df) > 2
    segment_df = sort_one_candle_peaks(segment_df, invert=False)

    # init current block 第一个segment假设为enter segment
    block_high = block_highest = current_high = current_highest = segment_df.peak.iloc[1:3].max()
    block_low = block_lowest = current_low = current_lowest = segment_df.peak.iloc[1:3].min()

    enter_date = segment_df.datetime.iloc[0]
    start_date = segment_df.datetime.iloc[1]
    segment_num = 2

    # 初始化block表
    columns = ['enter_date', 'start_date', 'block_high', 'block_low',
               'block_highest', 'block_lowest', 'segment_num']
    df = pd.DataFrame(columns=columns)

    for row in segment_df[3:].itertuples():
        index = row.Index
        if row.type == 'high':  # 顶
            if row.peak < block_low:  # 第三类卖点，新的中枢开始
                # 代码有5行冗余
                insert_row = pd.DataFrame([[enter_date, start_date, block_high, block_low,
                                            block_highest, block_lowest, segment_num]], columns=columns)
                df = df.append(insert_row, ignore_index=True)

                enter_date = segment_df.datetime.iloc[index - 2]
                start_date = segment_df.datetime.iloc[index - 1]
                segment_num = 1
                block_high = block_highest = current_high = current_highest = row.peak
                if segment_df.type.iloc[index - 1] == 'low':
                    pass  # TODO 测试代码
                assert segment_df.type.iloc[index - 1] == 'low'
                block_low = block_lowest = current_low = current_lowest = segment_df.peak.iloc[index - 1]
            else:
                block_low = current_low
                current_high = min(block_high, row.peak)
                block_lowest = current_lowest
                current_highest = max(block_highest, row.peak)
        else:
            if row.peak > block_high:  # 第三类买点，新的中枢开始
                insert_row = pd.DataFrame([[enter_date, start_date, block_high, block_low,
                                            block_highest, block_lowest, segment_num]], columns=columns)
                df = df.append(insert_row, ignore_index=True)

                enter_date = segment_df.datetime.iloc[index - 2]
                start_date = segment_df.datetime.iloc[index - 1]
                segment_num = 1
                assert segment_df.type.iloc[index - 1] == 'high'
                block_high = block_highest = current_high = current_highest = segment_df.peak.iloc[index - 1]
                block_low = block_lowest = current_low = current_lowest = row.peak
            else:
                block_high = current_high
                current_low = max(block_low, row.peak)
                block_highest = current_highest
                current_lowest = min(block_lowest, row.peak)

        segment_num = segment_num + 1

    # record last block
    insert_row = pd.DataFrame([[enter_date, start_date, block_high, block_low, block_highest,
                                block_lowest, segment_num]], columns=columns)
    df = df.append(insert_row, ignore_index=True)
    return df


# TODO 增加对 删除连续高低点；删除低点比高点高的段；删除高低点较近的段 的测试用例


def sort_one_candle_peaks(peak_df, invert=True):
    """
    对同一个k线的极值点进行排序
    :param peak_df: pd.DataFrame columns=[datetime, peak, type] 要将datetime设置为index
    :param invert: default True, 按 high， high和low， low排列
    :return: pd.DataFrame columns=[datetime, peak, type]
    """
    # 检测输入类型
    assert isinstance(peak_df, pd.DataFrame)
    assert peak_df.index.name is None

    df = peak_df.copy()  # 不对输入数据进行操作，有可能改变原始数据
    df['sn'] = range(len(df))

    # index_name = peak_df.index.name
    # if index_name is None:
    #     df.set_index('datetime')

    # 不存在同一个k线上有极值点的处理
    for row in df[df.index.duplicated()].itertuples():
        try:
            if df.type.iloc[row.sn] != df.type.iloc[row.sn + 1] and invert:
                df.iloc[row.sn], df.iloc[row.sn - 1] = df.iloc[row.sn - 1], df.iloc[row.sn]
        except IndexError:
            log.info('Last kline has both high and low as peaks!')

    del df['sn']

    # if index_name is None:
    #     df.reset_index(inplace=True)
    return df


def remove_fake_peaks(peak_df):
    """
    删除比相邻极点高的低点和比相邻极点低的高点，包括了高点/低点连续出现的情况
    :param peak_df: pd.DataFame, columns=[datetime, peak, type]
    :return: pd.DataFame, columns=[datetime, peak, type]
    """

    # 检测输入类型
    assert isinstance(peak_df, pd.DataFrame)

    df = peak_df.copy()  # 不对输入数据进行操作，有可能改变原始数据
    len_before = len(df)
    len_after = len_before - 1  # 为了循环能执行至少一次

    while len_after < len_before:
        diff_left = df.peak.diff()
        diff_right = df.peak.diff(-1)

        # 和前一个极值点点比较，没有比较的情况假设是合理的极值点
        diff_left.iloc[0] = -1 if df.type.iloc[0] == 'low' else 1
        diff_right.iloc[-1] = -1 if df.type.iloc[-1] == 'low' else 1

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
    :param peak_df: pd.DataFame, columns=[peak, type]
    :return: pd.DataFame, columns=[peak, type]
    """
    df = peak_df.copy()

    df = remove_fake_peaks(df)
    df = sort_one_candle_peaks(df)
    df = remove_fake_peaks(df)

    # df.to_excel('segment_ww.xlsx')
    # TODO 极值点离得较近的情况

    return df


# TODO 参数从datetime索引修改为序列号索引，需要测试TsBlock是否也需要修改
def get_peaks_from_segments(segment_df):
    """
    only calculate the peaks of kline value of high and low
    :param segment_df: pd.DataFame, 按照datetime升序排列， 索引从0-N
            columns=['datetime', 'peak', 'type']
    :return: pd.DataFame, columns=['datetime', 'peak', 'type']
        peak        high,low的极值点
        type        'high' or 'low'
    """

    df = segment_df.copy()
    # df.sort_values('datetime', inplace=True)
    # df.reset_index(drop=True, inplace=True)
    high_df = df[df['type'] == 'high']
    low_df = df[df['type'] == 'low']

    # 值相等的极值点全部保留
    high_df = high_df.iloc[signal.argrelextrema(high_df.peak.values, np.greater_equal)]
    low_df = low_df.iloc[signal.argrelextrema(-low_df.peak.values, np.greater_equal)]

    peak_df = high_df.append(low_df).sort_index()

    # 添加被错误删除的点，即两个端点之间还有更高的高点和更低的低点

    x = peak_df.reindex(df.index, method='bfill')
    y = peak_df.reindex(df.index, method='ffill')

    # 修正端点不是极值点的错误
    b_compare = x.peak - df.peak
    f_compare = y.peak - df.peak
    b1 = np.logical_and(np.logical_and(b_compare > 0, x.type == 'low'), segment_df.type == 'low')
    b2 = np.logical_and(np.logical_and(b_compare < 0, x.type == 'high'), segment_df.type == 'high')
    b3 = np.logical_and(np.logical_and(f_compare > 0, y.type == 'low'), segment_df.type == 'low')
    b4 = np.logical_and(np.logical_and(f_compare < 0, y.type == 'high'), segment_df.type == 'high')
    bflag = np.logical_or(np.logical_or(b1, b2), np.logical_or(b3, b4))

    peak_df = peak_df.append(segment_df[bflag.values]).sort_index()
    peak_df.reset_index(drop=True, inplace=True)
    return peak_df


def get_peaks_from_hq(hq_df):
    """
    only calculate the peaks of kline value of high and low
    :param hq_df: pd.DataFame, 按照datetime升序排列， 索引从0-N
            columns=['datetime', 'high', 'low']
    :return: pd.DataFame, columns=['datetime', 'peak', 'type']
        peak        high,low的极值点
        type        'high' or 'low'
    """
    df = hq_df.copy()
    # df.sort_values('datetime', inplace=True)
    # df.reset_index(drop=True, inplace=True)
    high_df = df.rename(columns={'high': 'peak'})
    del high_df['low']
    high_df['type'] = 'high'

    # low_df = lower.low.to_frame('peak')
    low_df = df.rename(columns={'low': 'peak'})
    del low_df['high']
    low_df['type'] = 'low'

    # 值相等的极值点全部保留
    high_df = high_df.iloc[signal.argrelextrema(high_df.peak.values, np.greater_equal)]
    low_df = low_df.iloc[signal.argrelextrema(-low_df.peak.values, np.greater_equal)]

    peak_df = high_df.append(low_df).sort_index()
    peak_df.reset_index(drop=True, inplace=True)
    # peak_df.to_excel('peak_ww.xlsx')

    return peak_df


def get_history_hq(code, start_date=None, end_date=None, freq='d'):
    """
    get history bar from external api
    :param code:
    :param start_date: datetime
    :param end_date: datetime
    :param freq: '5m', '30m', 'd', 'w'
    :return: pd.DataFrame columns=['high', 'low', 'serial_number]
    """
    if get_history_hq_api:
        temp_hq_df = get_history_hq_api(code=code, start=start_date, end=end_date, freq=freq)
        return temp_hq_df[['high', 'low']]
    else:  # only for test
        return None


# --------------------------往数据库插入数据-------------------------------------
def build_base_segments(symbol, frequency, instrument='index'):
    """
    对行情数据进行分段处理
    :param symbol: 交易代码
    :param frequency: 频率值，从0-9，从tick到year
    :param instrument: 交易品种类型 future option stock bond convertible index
    :return: True 还需要后续处理，不需要后续处理
    """

    segment_cursor = conn['segment']
    inst_cursor = conn[instrument]

    # 读取高一级别极值点作为处理的起点
    filter_dict = {'symbol': symbol, 'frequency': {'$gte': frequency}}
    last_doc = segment_cursor.find_one(filter_dict, sort=[('datetime', DESCENDING)], skip=1)

    filter_dict = {'symbol': symbol}
    if last_doc is None:
        # filter_dict['datetime'] = {'$lte': datetime(2005, 10, 7)}
        log.info("Build {} future segment from trade beginning.".format(symbol))
    else:
        update = last_doc['datetime']
        # filter_dict['datetime'] = {'$gte': update, '$lte': datetime(2009, 2, 24)}
        filter_dict['datetime'] = {'$gte': update}
        log.info("Build {} future segment from {}".format(symbol, update))

    # 从数据库读取所需数据
    projection = {'_id': 0, 'datetime': 1, 'high': 1, 'low': 1}

    hq = inst_cursor.find(filter_dict, projection=projection).sort([("datetime", ASCENDING)])
    hq_df = pd.DataFrame(list(hq))
    if hq_df.empty:
        log.debug('{} hq data:{} is empty!'.format(symbol, FREQ[frequency]))
        return False
    # 剔除掉成交价格为0的数据
    hq_df = hq_df[hq_df['low'] > 1e-10]
    if hq_df.empty:
        log.debug('{} hq data:{} is empty!'.format(symbol, FREQ[frequency]))
        return False

    # hq_df 需要按 datetime 升序排列, 返回的序列索引0-N
    peak_df = get_peaks_from_hq(hq_df)

    if peak_df.empty:
        log.debug('{} peak:{} data is empty.'.format(symbol, FREQ[frequency]))
        return False

    # 同一根k线出现极值点的情况
    if last_doc is None or peak_df.datetime[1] != peak_df.datetime[0]:
        log.debug('Do not judge first two peak recorders')
    else:
        log.debug('Two peak of {} in one day {}.'.format(symbol, peak_df.datetime[0]))
        if peak_df.type[0] == last_doc['type']:
            peak_df.drop(1, inplace=True)
        else:
            peak_df.drop(0, inplace=True)

    segment_df = get_segments_from_peaks(peak_df)
    if segment_df.empty:
        log.debug('{} segment:{} data is empty.'.format(symbol, FREQ[frequency]))
        return False

    segment_df['symbol'] = symbol
    segment_df['frequency'] = frequency

    origin_segment = segment_cursor.find(filter_dict, sort=[('datetime', ASCENDING)])
    origin_segment_df = pd.DataFrame(list(origin_segment))

    # 没有记录直接插入数据
    if origin_segment_df.empty:
        result = segment_cursor.insert_many(segment_df.to_dict('records'))

        if result.acknowledged:
            log.debug('{} segment:{} data insert success.'.format(symbol, FREQ[frequency]))
            return True  # 形成新的segment，需要后续进行处理block
        else:
            log.warning('{} segment:{} data insert failure.'.format(symbol, FREQ[frequency]))
            return False

    index = ['datetime', 'type']
    df1 = segment_df.set_index(index).sort_index()
    df2 = origin_segment_df.set_index(index).sort_index()

    rm_index = df2.index.difference(df1.index)

    if rm_index.empty:
        log.debug('Do not delete any historical {} segment:{} data .'.format(symbol, FREQ[frequency]))
    else:
        log.debug('Delete {} historical {} segment:{} data .'.format(len(rm_index), symbol, FREQ[frequency]))

        # if len(in_index) != len(segment_df) - 1:
        #     pass
        # 删除未确定数据
        result = segment_cursor.delete_many({'_id': {'$in': df2.loc[rm_index, '_id'].to_list()}})

        if result.acknowledged:
            log.debug('{} segment:{} data delete {} success.'.format(symbol, FREQ[frequency], len(rm_index)))
        else:
            log.warning('{} segment:{} data delete {} failure.'.format(symbol, FREQ[frequency], len(rm_index)))
            return False

    in_index = df1.index.difference(df2.index)
    segment_df = df1.loc[in_index].reset_index()
    if segment_df.empty:
        log.debug('Do not build any  {} segment:{} data .'.format(symbol, FREQ[frequency]))
        return False

    result = segment_cursor.insert_many(segment_df.to_dict('records'))

    if result.acknowledged:
        log.debug('{} segment:{} data insert {} success.'.format(symbol, FREQ[frequency], len(segment_df)))
        return True
    else:
        log.warning('{} segment:{} data insert {} failure.'.format(symbol, FREQ[frequency], len(segment_df)))
        return False


def build_high_level_segments(symbol, frequency):
    """
    对低级别段进行处理得到高级别段
    :param symbol: 交易代码
    :param frequency: 频率值，从0-9，从tick到year
    :return: True 还需要后续处理，不需要后续处理
    """
    segment_cursor = conn['segment']

    # 读取高一级别极值点作为处理的起点
    filter_dict = {'symbol': symbol, 'frequency': {'$gte': frequency}}
    last_doc = segment_cursor.find_one(filter_dict, sort=[('datetime', DESCENDING)], skip=2)

    if last_doc is None:
        log.info("Build {} future segment from trade beginning.".format(symbol))
    else:
        update = last_doc['datetime']
        # filter_dict['datetime'] = {'$gte': update, '$lte': datetime(2012, 9, 7)}
        filter_dict['datetime'] = {'$gte': update}
        log.info("Build {} future segment from {}".format(symbol, update))

    # 从数据库读取所需数据
    filter_dict['frequency'] = {'$gte': frequency - 1}
    low_segment = segment_cursor.find(filter_dict).sort([("datetime", ASCENDING)])
    low_segment_df = pd.DataFrame(list(low_segment))
    if low_segment_df.empty:
        log.debug('{} segment data:{} is empty!'.format(symbol, FREQ[frequency]))
        return False

    peak_df = get_peaks_from_segments(low_segment_df)

    if peak_df.empty:
        log.debug('{} peak:{} data is empty.'.format(symbol, FREQ[frequency]))
        return False

    if last_doc is None or peak_df.datetime[1] != peak_df.datetime[0]:
        log.debug('Do not judge first two peak recorders')
    else:
        log.debug('Two peak of {} in one day {}.'.format(symbol, peak_df.datetime[0]))
        if peak_df.type[0] == last_doc['type']:
            peak_df.drop(1, inplace=True)
        else:
            peak_df.drop(0, inplace=True)

    segment_df = get_segments_from_peaks(peak_df)
    if segment_df.empty:
        log.debug('{} segment:{} data is empty.'.format(symbol, FREQ[frequency]))
        return False

    origin_segment_df = low_segment_df[low_segment_df['frequency'] == frequency]

    # 没有记录直接更新数据
    if origin_segment_df.empty:
        result = segment_cursor.update_many({'_id': {'$in': segment_df['_id'].to_list()}},
                                            {'$set': {'frequency': frequency}})

        if result.acknowledged:
            log.debug('{} segment:{} data update {} success.'.format(symbol, FREQ[frequency], len(segment_df)))
            return True  # 形成新的segment，需要后续进行处理block
        else:
            log.warning('{} segment:{} data update failure.'.format(symbol, FREQ[frequency]))
            return False

    segment_df.set_index('_id', inplace=True)
    origin_segment_df.set_index('_id', inplace=True)

    down_index = origin_segment_df.index.difference(segment_df.index)

    if down_index.empty:
        log.debug('Do not degrade any historical {} segment:{} data .'.format(symbol, FREQ[frequency]))
    else:
        log.debug('Degrade {} historical {} segment:{} data .'.format(len(down_index), symbol, FREQ[frequency]))

        result = segment_cursor.update_many({'_id': {'$in': down_index.to_list()}},
                                            {'$set': {'frequency': frequency - 1}})

        if result.acknowledged:
            log.debug('{} segment:{} data degrade {} success.'.format(symbol, FREQ[frequency], len(down_index)))
        else:
            log.warning('{} segment:{} data degrade {} failure.'.format(symbol, FREQ[frequency], len(down_index)))
            return False

    segment_df = segment_df[segment_df['frequency'] < frequency]
    up_index = segment_df.index

    if up_index.empty:
        log.debug('Do not upgrade any historical {} segment:{} data .'.format(symbol, FREQ[frequency]))
        return False

    result = segment_cursor.update_many({'_id': {'$in': up_index.to_list()}},
                                        {'$set': {'frequency': frequency}})

    if result.acknowledged:
        log.debug('{} segment:{} data upgrade {} success.'.format(symbol, FREQ[frequency], len(up_index)))
        return True
    else:
        log.debug('{} segment:{} data upgrade {} failure.'.format(symbol, FREQ[frequency], len(up_index)))
        return False


def build_blocks(symbol, frequency):
    """
    只对单交易品种的一个频率进行处理
    :param symbol: 交易代码
    :param frequency: 频率值，从0-9，从tick到year
    :return:
    """
    # conn = connect_mongo('quote')
    block_cursor = conn['block']
    segment_cursor = conn['segment']

    filter_dict = {'symbol': symbol, 'frequency': frequency}
    last_2_docs = block_cursor.find(filter_dict, sort=[('enter_date', -1)], limit=2)

    #  只有一个记录或者没有记录要从头开始取数据
    last_2_docs_df = pd.DataFrame(list(last_2_docs))
    filter_dict['frequency'] = {'$gte': frequency}
    if last_2_docs_df.empty or len(last_2_docs_df) == 1:
        # filter_dict['datetime'] = {'$lte': datetime(2000, 5, 31)}
        log.info("Build {} future block from trade beginning.".format(symbol))
    else:
        update = last_2_docs_df['enter_date'].iloc[0]
        filter_dict['datetime'] = {'$gte': update}
        log.info("Build {} block from {}".format(symbol, update))

    segments = segment_cursor.find(filter_dict, sort=[('datetime', 1)])
    segment_df = pd.DataFrame(list(segments))
    if segment_df.empty or len(segment_df) < 3:
        log.debug('{} segment data:{} is not enough!'.format(symbol, FREQ[frequency]))
        return False

    block_df = identify_blocks(segment_df)

    if block_df.empty:
        log.info("There is no new {} block.".format(symbol))
        return False

    block_df['frequency'] = frequency
    block_df['symbol'] = symbol

    # 加入第一行数据 如果只有一个记录！！！！！
    length = len(last_2_docs_df)
    if not last_2_docs_df.empty:
        # 需要把'_id'删除
        df = last_2_docs_df.copy()
        del df['_id']
        if length == 2:
            block_df = pd.concat([df.tail(1), block_df], ignore_index=True, sort=False)

    block_df = identify_blocks_relation(block_df)
    block_dict = block_df.to_dict('records')

    if not last_2_docs_df.empty:
        last_doc_s = df.iloc[-1]
        if length == 1:
            first_doc_s = block_df.iloc[0]
        else:
            first_doc_s = block_df.iloc[1]
            del block_dict[0]

        last_doc_s = last_doc_s.reindex_like(first_doc_s)
        if last_doc_s.equals(first_doc_s):
            del block_dict[0]
        else:
            record_id = last_2_docs_df['_id'].iloc[0]
            # record_id = ObjectId(record_id.values[0])
            block_cursor.delete_one({'_id': record_id})

    if not block_dict:
        log.debug('{} block:{} data do not need update.'.format(symbol, FREQ[frequency]))
        return

    result = block_cursor.insert_many(block_dict)

    if result.acknowledged:
        log.debug('{} block:{} data update success.'.format(symbol, FREQ[frequency]))
        return True
    else:
        log.warning('{} block:{} data update failure.'.format(symbol, FREQ[frequency]))
        return False


def build_all_blocks():
    # 更新指数数据
    # build_future_index()

    # 先从指数分析，期货只分析XX00指数合约和XX88连续合约
    # Connect to MongoDB
    # conn = connect_mongo('quote')
    # segment_cursor = conn['segment']
    index_cursor = conn['index']

    filter_dict = {'symbol': {'$regex': '(8|0){2}$'}, 'datetime': {"$gt": datetime.today() - timedelta(360)}}
    symbols = index_cursor.distinct('symbol', filter_dict)

    if not isinstance(symbols, list) or len(symbols) == 0:
        print("Don't find any trading symbols in index collection!")
        return

    for symbol in symbols:
        # 从日线频率开始[ 5, 6, 7] 只处理日线 周线 月线数据,这里的频率只是级别分类，不代表字面意义
        frequency = 5
        # 是否形成新的段，形成新的段对block更新，有新的block，计算block之间的关系
        #              同时判断高级别段是否形成
        bflag = build_base_segments(symbol, frequency)
        frequency += 1
        while bflag and frequency < 10:
            bflag = build_high_level_segments(symbol, frequency)
            frequency += 1
            # if bflag:
            #     build_blocks(symbol, frequency)
            #     frequency += 1


if __name__ == "__main__":
    from datetime import datetime, timedelta

    # from src.features.block import TsBlock

    # start = datetime(2018, 6, 29)
    # end = datetime(2019, 3, 1)

    # build_segments()
    # build_high_level_segments('CF00', 9)
    # build_base_segments('CF00', 5)
    # build_blocks('A88', 5)
    build_all_blocks()
    # observation = 250
    # start = today - timedelta(observation)
    # end = today - timedelta(7)

    # block = TsBlock("SRL8")
    # peak = block.get_peaks(start=start, end=end)
    # segment = block.get_segments(start=start)
    # segment = block.get_segments(freq='w')
    # block_df = block.get_blocks()

    # block_w = block.get_current_status(freq='m')
    # block_d = block.get_current_status(start=start)
    # segment_w = block.get_segments(start=start, freq='w')
    # segment_d = block.get_segments(start=start)

    # TODO external api 如何使用
    # ml8 2018 9-10month  发现错误的段，低点比高点高·······························

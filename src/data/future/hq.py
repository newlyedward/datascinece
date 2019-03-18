# -*- coding: utf-8 -*-
import re

import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

from src.data.future.utils import get_file_index_needed
from src.data.setting import CODE2NAME_PATH, DATE_PATTERN
from src.log import LogHandler
from src.data.util.crawler import get_post_text
from src.data.future.setting import RAW_HQ_DIR, PROCESSED_HQ_DIR

log = LogHandler('future.log')

DCE_COLUMNS = ['open', 'high', 'low', 'close', 'pre_settle', 'settle', 'change1', 'change2', 'volume', 'open_interest',
               'oi_chg', 'turnover']
DCE_OPTION_COLUMNS = ['open', 'high', 'low', 'close', 'pre_settle', 'settle', 'change1', 'change2', 'delta', 'volume',
                      'open_interest', 'oi_chg', 'turnover', 'exercise_volume']


def get_dce_hq_by_date(date: datetime, code='all', category=0):
    """
    获取大连商品交易所日交易数据 20050104 期货数据起始日 2017331 期权数据起始日
    url = 'http://www.dce.com.cn//publicweb/quotesdata/dayQuotesCh.html'
    url = 'http://www.dce.com.cn/publicweb/quotesdata/exportDayQuotesChData.html'

    获取上海商品交易所日交易数据 20020108/20090105 期货数据起始日（还可以往前取） 2018921 期权数据起始日
    http://www.shfe.com.cn/data/dailydata/kx/kx20190318.dat
    http://www.shfe.com.cn/data/dailydata/option/kx/kx20190315.dat

    获取郑州商品交易所日交易数据
    http://www.czce.com.cn/cn/DFSStaticFiles/Future/2019/20190314/FutureDataDaily.txt
    http://www.czce.com.cn/cn/DFSStaticFiles/Future/2019/20190314/FutureDataDaily.htm
    datetime.date(2015, 9, 19)
    http://www.czce.com.cn/cn/exchange/2015/datadaily/20150821.htm
    datetime.date(2010, 8, 24)
    http://www.czce.com.cn/cn/exchange/jyxx/hq/hq20100806.html
    datetime.date(2005, 4, 29)

    form.data =
        dayQuotes.variety	    : all/y/m
        dayQuotes.trade_type	: 0/1  0:期货 1:期权
        year	    : 2019
        month	    : 2 实际月份-1
        day	        : 14
        exportFlag  : txt excel:xls
    :param code: 商品代码
    :param date: datetime
    :param category: 行情类型, 0期货 或 1期权
    :return 不成功返回0

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    url = 'http://www.dce.com.cn/publicweb/quotesdata/exportDayQuotesChData.html'
    form_data = {'dayQuotes.variety': code,
                 'dayQuotes.trade_type': category,
                 'year': date.year,
                 'month': date.month - 1,
                 'day': date.day,
                 'exportFlag': 'txt'}
    # TODO 确认最早的数据是否从 datetime(2005, 1, 4)开始
    return get_post_text(url, form_data)


def get_dce_hq_by_dates(start=datetime(2005, 1, 4), end=datetime.today(), category=0):
    """
    根据日期连续下载大连商品交易所日交易数据 20050104 期货数据起始日 2017331 期权数据起始日
    :param start: datetime
    :param end: datetime
    :param start:
    :param category: 行情类型, 0期货 或 1期权
    :return pd.DataFrame
    大商所日交易数据(DataFrame):
        symbol        合约代码
        date          日期
        open          开盘价
        high          最高价
        low           最低价
        close         收盘价
        volume        成交量
        open_interest   持仓量
        turnover       成交额
        settle        结算价
        pre_settle    前结算价
        variety       合约类别
        或
   大商所每日期权交易数据
        symbol        合约代码
        date          日期
        open          开盘价
        high          最高价
        low           最低价
        close         收盘价
        pre_settle      前结算价
        settle         结算价
        delta          对冲值
        volume         成交量
        open_interest     持仓量
        oi_change       持仓变化
        turnover        成交额
        implied_volatility 隐含波动率
        exercise_volume   行权量
        variety        合约类别
    或 None(给定日期没有交易数据)

    """
    assert category in [0, 1]

    start = datetime(2011, 1, 1) if category == 0 else datetime(2017, 3, 31) \
        if start is None else start
    end = datetime.today() if end is None else start

    target = RAW_HQ_DIR[category] / 'dce'

    file_index = get_file_index_needed(target, 'txt', start=start, end=end)

    if file_index.empty:
        return False

    for date in file_index:
        print(date)
        text = get_dce_hq_by_date(date, category=category)
        date_str = date.strftime('%Y%m%d')
        file_path = target / '{}_daily.txt'.format(date_str)
        if len(text) < 4:
            log.info('{} is fail, status: {}'.format(date_str, text))
            continue
        file_path.write_text(text)
        time.sleep(np.random.rand() * 90)

    return True


def construct_dce_hq(start=datetime(2005, 1, 2), end=None, category=0):
    """
    hdf5 文件知道如何插入记录，暂时只能添加在文件尾部，因此要保证 历史数据连续
    :param category: 0 期货 1 期权
    :param start:
    :param end:
    :return:
    """
    assert category in [0, 1]

    end = datetime.today() if end is None else end

    df = pd.read_csv(CODE2NAME_PATH, encoding='gb2312', header=0,
                     usecols=['code', 'market', 'exchange']).dropna()

    df = df[df['market'] == 'dce']

    assert not df.empty

    source = RAW_HQ_DIR[category] / 'dce'

    # target = [PROCESSED_HQ_DIR[category] / '{}.day'.format(x.code)
    #           for x in df.itertuples()]

    target = PROCESSED_HQ_DIR[category] / 'dce_future_hq.day'
    update = None

    if target.exists():
        # 找最后一个记录的时间，所有商品相同
        last = pd.read_hdf(target, 'table', start=-1)
        update = last.index[0] + timedelta(1)

        if update > end:
            return


    # updates = []
    # for file in target:
    #     if file.exists():
    #         last = pd.read_hdf(target, 'table', start=-1)
    #         updates.append(last.index[0] + timedelta(1))
    #     else:
    #         updates = np.nan
    #
    # df['update'] = updates
    # df['filepath'] = target
    # del df['market']
    # update = df['update'].min()
    #
    # if update > end:
    #     return

    # 保证每次将数据更新需要的程度
    get_dce_hq_by_dates(start=update, end=end, category=category)

    # concat raw data from specific date
    file_df = pd.DataFrame([(pd.to_datetime(re.search(DATE_PATTERN, x.name)[0]), x)
                            for x in source.glob('*.txt')], columns=['datetime', 'filepath'])
    file_df.set_index('datetime', inplace=True)
    # TODO 由于断电，死机故障没有完成所有hdf数据转换，下一次可能丢失掉一些品种的历史数据转换，将数据全部删除重新生成
    if update:
        file_df.query("index>=Timestamp('{}') & index<=Timestamp('{}')".format(update, end),
                      inplace=True)

    if file_df.empty:
        return

    dtype = {'开盘价': 'float64', '最高价': 'float64', '最低价': 'float64', '收盘价': 'float64',
             '结算价': 'float64', '涨跌': 'float64', '涨跌1': 'float64', '成交量': 'float64',
             '持仓量': 'float64', '持仓量变化': 'float64', '成交额	': 'float64',
             "商品名称": 'object', "交割月份": 'object'}

    # 如何插入日期
    def insert_date(row):
        df = pd.read_csv(row.filepath, encoding='gb2312', header=0, index_col=False,
                         dtype=dtype, sep='\s+', thousands=',')

        df['日期'] = row.index
        return df

    frames = [insert_date(x) for x in file_df.itertuples()]

    if len(frames) == 0:
        return

    spread_df = pd.concat(frames, ignore_index=True)

    df.set_index('exchange', inplace=True)
    spread_df = spread_df.join(df, on='商品名称')

    spread_df.set_index('日期', inplace=True)
    try:
        spread_df.to_hdf(target, 'table', format='table',
                         append=True, complevel=5, complib='blosc')
    except ValueError:  # TypeError
        log.warning('{}'.format(spread_df.columns))


if __name__ == '__main__':
    start_dt = datetime(2014, 12, 21)
    end_dt = datetime(2005, 1, 15)
    print(datetime.now())
    # get_dce_hq_by_dates()
    construct_dce_hq(start=datetime(2005, 1, 2), end=end_dt, category=0)
    # df = pd.read_csv(file_path, encoding='gb2312', sep='\s+')
    # df = get_future_hq('M', start=start_dt, end=None)
    print(datetime.now())

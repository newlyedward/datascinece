# -*- coding: utf-8 -*-

import datetime as dt

from src.log import LogHandler

from src.data.tdx.hq import get_future_day_hq, get_future_min_hq
from src.data.tdx.basic import get_future_basic

log = LogHandler('tdx.log')

# TODO 周期转换的处理， 日内的数据由于有夜盘，处理起来较复杂
# tdx处理是夜盘数据算作下一个交易日，resample函数处理起来会按照时间排序
# conversion = {'open': 'first', 'high': 'max', 'low': 'min',
#               'close': 'last', 'openInt' : 'last', 'volume': 'sum',
#               'comment': 'last'}

# m30 = df.resample('30min', closed='right', label='right).apply(conversion).dropna()


def get_future_hq(code, start=dt.datetime(1970, 1, 1), end=None, freq='d'):
    """
    根据contractid找到对应的market，调用对应的行情函数
    :param code: IL8 主力合约 IL9 期货指数
    :param start: 开始时间
    :param end:   结束时间
    :param freq: 周期'1m'，'5m'
    :return:
    """
    future_basic_info = get_future_basic()

    # 根据交易品种找到对应的市场
    length = len(code)
    if length == 4:
        symbol = code[:2].upper()
    elif length == 3:
        symbol = code[0].upper()
    else:
        log.warning(code, 'is not listed!')
    market = future_basic_info.loc[symbol, 'market']

    log.info('freq={}'.format(freq))

    if freq == 'd':
        return get_future_day_hq(market=market, code=code, start=start, end=end)
    if freq in ('1m', '5m'):
        return get_future_min_hq(market=market, code=code, start=start, end=end, freq=freq)

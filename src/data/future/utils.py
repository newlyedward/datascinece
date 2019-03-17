# -*- coding: utf-8 -*-
from src.log import LogHandler
from src.data.tdx import get_future_hq


log = LogHandler('future.log')


# 期货历史交易日历，返回铜期货指数交易日期，2000/1/4开始
def get_future_calender(start=None, end=None):
    df = get_future_hq('cuL9', start=start, end=end)
    if df is None:
        df = get_future_hq('cuL9', start=start)
    return df.index


if __name__ == '__main__':
    from datetime import datetime
    get_future_calender(datetime(2008,12,3), datetime(2009,1,8))

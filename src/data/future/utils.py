# -*- coding: utf-8 -*-
import re

import pandas as pd
from datetime import datetime

from src.data.setting import DATE_PATTERN
from src.log import LogHandler
from src.data.tdx import get_future_hq

log = LogHandler('future.log')


# 期货历史交易日历，返回铜期货指数交易日期，2000/1/4开始
def get_future_calender(start=None, end=None):
    df = get_future_hq('cuL9', start=start, end=end)
    if df is None:
        df = get_future_hq('cuL9', start=start)
    return df.index


def get_file_index_needed(target, ext, start=None, end=None):
    """

    :param target: 数据目录
    :param ext: 数据文件扩展名
    :param start: 需要数据的起始日期
    :param end:
    :return: pandas.core.indexes.datetimes.DatetimeIndex 日期的索引值
    """
    assert start <= end <= datetime.today()

    ret = pd.to_datetime([])

    try:
        trade_index = get_future_calender(start=start, end=end)
    except AttributeError:
        log.info('{} to {} are not in trading calender!'.format(start, end))
        return ret

    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        target.mkdir(parents=True, exist_ok=True)
        file_index = ret
    else:
        try:
            file_index = pd.to_datetime([re.search(DATE_PATTERN, x.name)[0]
                                         for x in target.glob('*.{}'.format(ext))])
        except TypeError:  # 目录存在但没有文件
            file_index = ret

    if file_index.empty:
        file_index = trade_index
    else:
        file_index = trade_index.difference(file_index)

    return file_index


if __name__ == '__main__':
    from datetime import datetime

    get_future_calender(datetime(2008, 12, 3), datetime(2009, 1, 8))

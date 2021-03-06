# -*- coding: utf-8 -*-
import re
import numpy as np
import pandas as pd

from log import LogHandler

log = LogHandler('util.log')


def convert_percent(value):
    """
    转换字符串百分数为float类型小数
    :param value:
    :return:
    """
    pattern = re.compile(r'-?([1-9]\d*\.\d*|0\.\d*[1-9]\d*|0?\.0+|0)')
    m = re.search(pattern, value)
    if m:
        new_value = float(m.group(0)) / 100
    else:
        new_value = np.nan

    return new_value


def count_percentile(value, data):
    if isinstance(data, pd.Series):
        s = data.copy()
        s.name = 'value'
        s.dropna(inplace=True)
        s = s.sort_values()
        s = s.reset_index(drop=True)
        s = s[s > value]
        if s.empty:
            return 0
        else:
            return s.index[0] / (s.index[-1] + 1) * 100
    log.warning('The data is type of {}, pd.Series type required!'.format(type(data)))
    return 0

# -*- coding: utf-8 -*-
import re
import numpy as np


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
# -*- coding: utf-8 -*-
"""
跟行情数据有关的特征值
"""
import pandas as pd
from src.data.util import read_mongo
from src.log import LogHandler

log = LogHandler('feature.future.log')


def get_future_index(code='', start='', end='', type=0):
    """

    :param end: datetime.datetime
    :param start: datetime.datetime
    :param code: 品种缩写 CU，RB 如果为''，返回全部品种的数据
    :param type: 指数类型 加权指数，主力指数，连续指数
    :return:
    """
    pass


if __name__ == '__main__':
    from datetime import datetime
    get_future_index('cu', start=datetime(2019, 1, 1))

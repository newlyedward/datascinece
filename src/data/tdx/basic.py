# -*- coding: utf-8 -*-
import os
from src.log import LogHandler
import pandas as pd

from .setting import tdx_dir

log = LogHandler(os.path.basename(__file__))


def get_future_basic():
    file_name = os.path.join(tdx_dir, 'T0002\hq_cache\code2name.ini')
    df = pd.read_csv(file_name,
                     index_col=0, names=['category', 'name', 'market'], header=None,
                     encoding='gb2312', usecols=[0, 1, 2])
    df.loc[df['market'] == 'CZ', 'market'] = 'cffex'
    df.loc[df['market'] == 'QD', 'market'] = 'dce'
    df.loc[df['market'] == 'QZ', 'market'] = 'czce'
    df.loc[df['market'] == 'QS', 'market'] = 'shfe'
    return df
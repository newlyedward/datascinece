# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime
import re

from src.data.future.utils import get_future_calender
from src.log import LogHandler
from src.data.util import get_html_text
from src.data.setting import raw_data_dir

log = LogHandler('future.log')

HEADER = ['日期', '品种', '上周库存小计', '上周库存期货', '本周库存小计', '本周库存期货', '库存增减小计',
          '库存增减期货', '上周可用库容量', '本周可用库容量', '可用库容量增减']


def get_inventory_from_shfe(date_str):
    """
    上海期货交易所指定交割仓库库存周报
    :param date_str: str of datetime
    :return: list
    """
    url_template = "http://www.shfe.com.cn/data/dailydata/{}weeklystock.dat"
    url = url_template.format(date_str)
    text = get_html_text(url, encoding='unicode')
    try:
        json_obj = json.loads(text)
    except:
        log.warning('Get {} inventory data fail. Status code: {}. Url: {}'.format(text, url))
        return []
    tradingday = json_obj['o_tradingday']

    data = []
    for idx, l in enumerate(json_obj['o_cursor']):
        if not re.match(r'\S+?\$\$Total$', l['WHABBRNAME']):
            continue
        data.append([tradingday, l['VARNAME'].split('$$')[0],
                     l['PRESPOTWGHTS'], l['PREWRTWGHTS'],
                     l['SPOTWGHTS'], l['WRTWGHTS'],
                     l['SPOTCHANGE'], l['WRTCHANGE'],
                     l['PREWHSTOCKS'], l['WHSTOCKS'], l['WHSTOCKCHANGE']])
    return data, text


def get_future_inventory(start=datetime(2007, 1, 5), end=datetime.today()):
    """

    :param start: 2007-01-05 上海期货交易所最早数据
    :param end:
    :return:
    """
    trade_index = get_future_calender(start=start, end=end)

    target = raw_data_dir / 'inventory/shfe'

    if not target.exists():
        target.mkdir()
        file_index = None
    else:
        file_index = pd.to_datetime([x.name[:-4] for x in target.glob('*.csv')])
        # file_index = pd.to_datetime([x.name[:-4] for x in target.glob('*.txt')])

    # TODO
    # FutureWarning: DatetimeIndex.offset    has    been    deprecated and will    be    removed in a    future
    # version; use    DatetimeIndex.freq    instead.
    #
    # shfe 只在每周最后一个交易日提供库存数据
    x = pd.Series(trade_index, index=trade_index)
    index = pd.DatetimeIndex(x.resample('W', label='left').last().dropna())

    if file_index is None:
        file_index = trade_index
    else:
        file_index = index.difference(file_index)

    for date in file_index:
        date_str = date.strftime('%Y-%m-%d')
        file_path = target / '{}.csv'.format(date_str)
        json_path = target / '{}.txt'.format(date_str)
        # 2014-05-23 shfe之前的数据不是jason格式，需要重新爬取
        if file_path.exists() or date < datetime(2014, 5, 23):
            continue

        table, text = get_inventory_from_shfe(date.strftime('%Y%m%d'))
        if len(table) != 0:
            print(date)
            spread_df = pd.DataFrame(table, columns=HEADER)
            spread_df.to_csv(str(file_path), index=False, encoding='gb2312')
            json_path.write_text(text)
        time.sleep(np.random.rand() * 90)
    return None


if __name__ == '__main__':
    start_dt = datetime(2014, 5, 23)
    end_dt = datetime(2019, 3, 31)
    print(datetime.now())
    get_future_inventory(start_dt, end_dt)
    print(datetime.now())

# 上海期货交易所会员成交及持仓排名表
# shfe 从2002开始有数据，全部json格式，字符使用unidcode编码

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
from src.data.setting import RAW_DATA_DIR

log = LogHandler('future.log')

HEADER = ['日期', '品种', '合约代码', '持买单量', '持卖单量']


def get_receipt_from_shfe(date_str):
    url_template = "http://www.shfe.com.cn/data/dailydata/kx/pm{}.dat"
    url = url_template.format(date_str, encoding='unicode')
    text = get_html_text(url)
    try:
        json_obj = json.loads(text)
    except:
        log.warning('Get {} receipt data fail. Status code: {}. Url: {}'.format(text, url))
        return []

    df = pd.DataFrame(json_obj['o_cursor'])
    data = df[['PRODUCTNAME', 'INSTRUMENTID', 'CJ2', 'CJ3']]
    x = df.loc[df['RANK'] > 20, ['PRODUCTNAME', 'INSTRUMENTID', 'CJ2', 'CJ3']]
    data = pd.DataFrame(x.values, index=[pd.to_datetime('20130315')] * len(x), columns=['品种', '合约代码', '持买单量', '持卖单量'])
    for idx, l in enumerate(json_obj['o_cursor']):
        if not re.match(r'\S+?\$\$Total$', l['WHABBRNAME']):
            continue
        data.append([tradingday, l['VARNAME'].split('$$')[0],
                     l['WRTWGHTS'], l['WRTCHANGE']])

    return data


def get_future_positions(start=datetime(2007, 1, 5), end=datetime.today()):
    """

    :param start: 2007-01-05 上海期货交易所最早数据
    :param end:
    :return:
    """
    trade_index = get_future_calender(start=start, end=end)

    target = RAW_DATA_DIR / 'receipt/shfe'
    # 只能建立一级目录，不能向上递归建立
    if not target.exists():
        target.mkdir()
        file_index = None
    else:
        file_index = pd.to_datetime([x.name[:-4] for x in target.glob('*.csv')])

    if file_index is None:
        file_index = trade_index
    else:
        file_index = trade_index.difference(file_index)

    for date in file_index:
        date_str = date.strftime('%Y-%m-%d')
        file_path = target / '{}.csv'.format(date_str)

        # 2014-05-23 shfe之前的数据不是jason格式，需要重新爬取
        if file_path.exists() or date < datetime(2014, 5, 23):
            continue

        table = get_receipt_from_shfe(date.strftime('%Y%m%d'))
        if len(table) != 0:
            print(date)
            spread_df = pd.DataFrame(table, columns=HEADER)
            spread_df.to_csv(str(file_path), index=False, encoding='gb2312')
        time.sleep(np.random.rand() * 90)
    return None


if __name__ == '__main__':
    print(datetime.now())
    get_future_positions()
    print(datetime.now())

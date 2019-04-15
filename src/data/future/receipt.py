# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime
import re

from src.data.future.setting import RECEIPT_DIR
from src.data.future.utils import get_download_file_index
from log import LogHandler
from src.util import get_html_text

log = LogHandler('future.log')

HEADER = ['日期', '品种', '期货仓单', '仓单变化']


def get_receipt_from_shfe(date_str):
    url_template = "http://www.shfe.com.cn/data/dailydata/{}dailystock.dat"
    url = url_template.format(date_str, encoding='unicode')
    text = get_html_text(url)
    try:
        json_obj = json.loads(text)
    except:
        log.warning('Get {} receipt data fail. Status code: {}. Url: {}'.format(text, url))
        return []
    tradingday = json_obj['o_tradingday']

    data = []
    for idx, l in enumerate(json_obj['o_cursor']):
        if not re.match(r'\S+?\$\$Total$', l['WHABBRNAME']):
            continue
        data.append([tradingday, l['VARNAME'].split('$$')[0],
                     l['WRTWGHTS'], l['WRTCHANGE']])

    return data, text


def get_future_receipts(start=datetime(2007, 1, 5), end=datetime.today()):
    """

    :param start: 2007-01-05 上海期货交易所最早数据
    :param end:
    :return: True 下载文件 False 没有下载文件
    """
    file_index = get_download_file_index(RECEIPT_DIR, 'csv', start=start, end=end)

    if file_index.empty:
        return False

    for date in file_index:
        date_str = date.strftime('%Y-%m-%d')
        file_path = RECEIPT_DIR / '{}.csv'.format(date_str)
        json_path = RECEIPT_DIR / '{}.txt'.format(date_str)

        # 2014-05-23 shfe之前的数据不是jason格式，需要重新爬取
        if file_path.exists() or date < datetime(2014, 5, 23):
            continue

        table, text = get_receipt_from_shfe(date.strftime('%Y%m%d'))
        if len(table) != 0:
            print(date)
            spread_df = pd.DataFrame(table, columns=HEADER)
            spread_df.to_csv(str(file_path), index=False, encoding='gb2312')
            json_path.write_text(text)
        time.sleep(np.random.rand() * 90)
    return True

if __name__ == '__main__':
    print(datetime.now())
    get_future_receipts()
    print(datetime.now())

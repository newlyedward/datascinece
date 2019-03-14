# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import re

from src.data.future.utility import get_future_calender
from src.log import LogHandler
from src.data.util import get_html_tree
from src.data.setting import raw_data_dir

log = LogHandler('future.spread.log')

columns = ["商品", "现货价格", "最近合约代码", "最近合约价格", "最近合约现期差1", "最近合约期现差百分比1", "主力合约代码",
           "主力合约价格", "主力合约现期差2", "主力合约现期差百分比2", "日期", "交易所"]


# columns = ['commodity', 'sprice', 'recent_code', 'recent_price', 'recent_basis', 'recent_basis_prt', 'dominant_code',
#            'dominant_price', 'dominant_basis', 'dominant_basis_prt', 'datetime', 'exchange']


def get_spreads_by_date(date_str: list):
    """

    :param date_str: list of datetime
    :return: list
    """
    url_template = "http://www.100ppi.com/sf/day-{}.html"
    url = url_template.format(date_str)
    html = get_html_tree(url)
    ele_list = html.xpath('//table[@id="fdata"]//tr[@align="center"] | //table[@id="fdata"]//tr/td[@colspan="8"]')
    ret = []
    if len(ele_list) == 0:
        return ret
    else:
        exchange = ""
        for ele in ele_list:
            if ele.tag == "td":
                exchange = ele.text
            elif ele.tag == "tr":
                raw_val = ele.xpath('./td/a/text()|./td/text()|.//td/font/text()')
                val = [re.findall(r'^(\S+)\xa0', val)[0] if re.match(r'\S+\xa0', val) else val
                       for val in raw_val if not re.match(r'^\s+$', val)]
                val.extend([date_str, exchange])
                ret.append(val)
            else:
                print("the data extracted from url has errors")
    return ret


def get_future_spreads(start, end=datetime.today()):
    trade_index = get_future_calender(start=start, end=end)

    target = raw_data_dir / 'spread'

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
        if file_path.exists():
            continue

        table = get_spreads_by_date(date_str)
        if len(table) != 0:
            print(date)
            spread_df = pd.DataFrame(table, columns=columns)
            spread_df.to_csv(str(file_path), index=False, encoding='gb2312')
        time.sleep(np.random.rand() * 180)
    return None

# 'D:\\Code\\test\\cookiercutter\\datascience\\datascinece\\src\\data\\future\\code2name.csv'
# 'D:\Code\test\cookiercutter\datascience\datascinece\data\raw\spread\2016-02-04.csv'

if __name__ == '__main__':
    # end_dt = datetime.today()
    start_dt = datetime(2016, 1, 2)
    end_dt = datetime(2016, 12, 31)
    print(datetime.now())
    get_future_spreads(start_dt, end_dt)
    print(datetime.now())
    # write_to_csv(df)

# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import time
from datetime import datetime
import re

from src.data.future.utils import get_future_calender
from src.log import LogHandler
from src.data.util import get_html_tree
from src.data.setting import raw_data_dir

log = LogHandler('future.log')

HEADER = ["商品", "现货价格", "最近合约代码", "最近合约价格", "最近合约现期差1", "最近合约期现差百分比1", "主力合约代码",
          "主力合约价格", "主力合约现期差2", "主力合约现期差百分比2", "日期", "交易所"]


# columns = ['commodity', 'sprice', 'recent_code', 'recent_price', 'recent_basis', 'recent_basis_prt', 'dominant_code',
#            'dominant_price', 'dominant_basis', 'dominant_basis_prt', 'datetime', 'exchange']


def get_spreads_from_100ppi(date_str):
    """
    http://www.100ppi.com/sf/  最新的期现表
    http://www.100ppi.com/sf/day-{}.html 历史期限表，填入%Y-%m-%d
    :param date_str: str of datetime
    :return: list
    """
    url_template = "http://www.100ppi.com/sf/day-{}.html"
    url = url_template.format(date_str)
    html = get_html_tree(url)
    ele_list = html.xpath('//table[@id="fdata"]//tr[@align="center"] | //table[@id="fdata"]//tr/td[@colspan="8"]')
    data = []
    if len(ele_list) == 0:
        return data
    else:
        exchange = ""
        for ele in ele_list:
            if ele.tag == "td":
                exchange = ele.text
            elif ele.tag == "tr":
                raw_val = ele.xpath('./td/a/text()|./td/text()|.//td/font/text()')
                val = [re.findall(r'^(\S+)\xa0', val)[0] if re.match(r'\S+\xa0', val) else val.strip()
                       for val in raw_val if not re.match(r'^[\r\n\t]+$', val)]
                if len(val) != 10:
                    log.warning('{} Spread data is not enough. Url:{}'.fromat(date_str, url))
                    return []
                val.extend([date_str, exchange])
                data.append(val)
            else:
                log.info("the data extracted from url has errors")
    return data


def get_future_spreads(start=datetime(2011, 1, 1), end=datetime.today()):
    """
    下载数据，存储为csv文件
    :param start: 2011-01-01 最早数据
    :param end:
    :return: None
    """
    assert start <= end
    try:
        trade_index = get_future_calender(start=start, end=end)
    except AttributeError:
        log.info('{} to {} are not in trading calender!'.format(start, end))
        return False

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

        table = get_spreads_from_100ppi(date_str)
        if len(table) != 0:
            print(date)
            spread_df = pd.DataFrame(table, columns=HEADER)
            spread_df.to_csv(str(file_path), index=False, encoding='gb2312')
        time.sleep(np.random.rand() * 90)
    return True


if __name__ == '__main__':
    # end_dt = datetime.today()
    start_dt = datetime(2017, 1, 1)
    end_dt = datetime(2019, 3, 31)
    print(datetime.now())
    get_future_spreads(start_dt, end_dt)
    print(datetime.now())
    # write_to_csv(df)

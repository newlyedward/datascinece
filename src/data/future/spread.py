# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import time
import re
from datetime import datetime, timedelta
from pymongo import ASCENDING, DESCENDING

from src.data import conn
from src.data.future.setting import SPREAD_DIR, NAME2CODE_MAP
from src.data.future.utils import get_download_file_index, get_exist_files, move_data_files
from log import LogHandler
from src.util import get_html_tree

log = LogHandler('future.log')

HEADER = ["商品", "现货价格", "最近合约代码", "最近合约价格", "最近合约现期差1", "最近合约期现差百分比1", "主力合约代码",
          "主力合约价格", "主力合约现期差2", "主力合约现期差百分比2", "日期", "交易所"]


# columns = ['commodity', 'sprice', 'recent_code', 'recent_price', 'recent_basis', 'recent_basis_prt', 'dominant_code',
#            'dominant_price', 'dominant_basis', 'dominant_basis_prt', 'datetime', 'exchange']


def download_spot_by_date(date_str):
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


def download_spot_by_dates(start=datetime(2011, 1, 1)):
    """
    下载数据，存储为csv文件
    :param start: 2011-01-01 最早数据
    :return: True 下载文件 False 没有下载文件
    """
    file_index = get_download_file_index(SPREAD_DIR, start=start)

    if file_index.empty:
        return False

    for date in file_index:
        date_str = date.strftime('%Y-%m-%d')
        file_path = SPREAD_DIR / '{}.csv'.format(date_str)
        if file_path.exists():
            continue

        table = download_spot_by_date(date_str)
        if len(table) != 0:
            print(date)
            spread_df = pd.DataFrame(table, columns=HEADER)
            spread_df.to_csv(str(file_path), index=False, encoding='gb2312')
        time.sleep(np.random.rand() * 5)
    return True


def insert_spot_to_mongo():
    """

    :return:
    """
    cursor = conn['spot_price']
    start = cursor.find_one({}, sort=[("datetime", DESCENDING)])
    if start is None:
        result = download_spot_by_dates()
    else:
        start = start['datetime']
        result = download_spot_by_dates(start=start)

    if not result:
        log.info('Data files are inserted before!')
        return

    # date_index = cursor.distinct('datetime', {'datetime': {'&gt': start}})

    file_df = get_exist_files(SPREAD_DIR)
    if start is not None:
        file_df = file_df[start + timedelta(1):]

    # if len(date_index) != 0:
    #     mongo_index = pd.to_datetime(date_index)
    #     file_index = file_df.index.difference(mongo_index)
    #     file_df = file_df.loc[file_index]

    if file_df.empty:
        log.info('Spot price is updated before!')
        return

    for row in file_df.itertuples():
        df = pd.read_csv(row.filepath, encoding='gb2312', header=0, index_col=False)
        spot_df = df[['商品', '现货价格']]
        spot_df.columns = ['code', 'spot']
        spot_df.loc[:, 'code'] = spot_df['code'].transform(lambda x: NAME2CODE_MAP['spread'][x])
        spot_df.loc[:, 'datetime'] = row.Index

        result = cursor.insert_many(spot_df.to_dict('records'))
        if result:
            print('{} spot price insert success.'.format(row.filepath.name))
            move_data_files(row.filepath)
        else:
            print('{} spot price insert failure.'.format(row.filepath.name))


if __name__ == '__main__':
    # end_dt = datetime.today()
    start_dt = datetime(2019, 3, 19)
    end_dt = datetime(2019, 3, 31)
    print(datetime.now())
    # download_spot_by_dates(start_dt)
    insert_spot_to_mongo()
    print(datetime.now())
    # write_to_csv(df)

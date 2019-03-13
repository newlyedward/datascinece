# -*- coding: utf-8 -*-
import datetime
import json
import time

import numpy as np
import pandas as pd

from src.log import LogHandler
from src.data.util import get_html_tree, get_html_text, convert_percent
from src.data.setting import raw_data_dir   # TODO 单文件运行时 import 错误
from src.data.jsl.setting import *

log = LogHandler('jsl.log')


def is_not_trading():
    begin_time = datetime.time(9, 30)
    end_noon = datetime.time(11, 30)
    begin_noon = datetime.time(13)
    end_time = datetime.time(15)
    current_weekday = datetime.datetime.weekday(datetime.datetime.now())
    current_time = datetime.datetime.time(datetime.datetime.now())

    return current_weekday in [0, 6] \
        or not (current_time in [begin_time, end_noon] or current_time in [begin_noon, end_time])


def get_convertible_list():
    file_path = raw_data_dir / "convertible_list.csv"
    file_name = str(file_path)

    # 文件存在并且不是交易时间，还需要判断文件足够新才行
    if file_path.exists() and is_not_trading():
        convertible_df = pd.read_csv(file_name, encoding='gb2312')
    else:
        convertible_df = pd.DataFrame([x['cell'] for x in json.loads(get_html_text(CBS_URL))['rows']])
        convertible_df['premium_rt'] = convertible_df['premium_rt'].apply(convert_percent)
        convertible_df['ytm_rt_tax'] = convertible_df['ytm_rt_tax'].apply(convert_percent)
        convertible_df['ytm_rt'] = convertible_df['ytm_rt'].apply(convert_percent)
        convertible_df.to_csv(file_name, encoding='gb2312')
    # TODO 将日期转换为字符 pd.to_datetime
    return convertible_df


def get_stock_detail(stock_cd):
    """
    https://www.jisilu.cn/data/stock/600831 抓取数据
    :param stock_cd: 股票代码
    :return: 未转换为数字的原始字符串list
    """
    # columns = ['sprice', 'increase_rt', 'market_value', 'increase_rt', 'debt_with_interest',
    #            'average_dividend_yield_5_years', 'dividend_yield', 'listed_dt', 'total_shares',
    #            'average_roe_5_years', 'sales_rt_5_years', 'profit_rt_5_years', 'net_profit_rt_5_years']
    dom_tree = get_html_tree(CBS_STOCK_DETAIL_URL + stock_cd)
    # pbe = dom_tree.xpath('//div[@class="item_desc"]/text()')  # 当前值没有
    return dom_tree.xpath('//table[@id="stock_detail"]/tr/td/span/text()')


def get_stocks_detail(stocks_id=None):
    """

    :param stocks_id: list or series 'sh603305 sz002478'
    :return: df
    """
    file_path = raw_data_dir / "stocks_detail.csv"
    file_name = str(file_path)

    if isinstance(stocks_id, pd.DataFrame):
        stocks_id = stocks_id['stock_id'].tolist()

    # TODO 文件存在并且不是交易时间，还需要判断文件足够新才行
    if file_path.exists() and is_not_trading():
        df = pd.read_csv(file_name, encoding='gb2312')
        # 根据stocks code 返回需要的数据
    else:
        stocks_detail = []
        convertible_df = get_convertible_list()
        for stock_id in convertible_df['stock_id'].tolist():
            time.sleep(np.random.rand()*5)
            try:
                stock_detail = get_stock_detail(stock_id[2:])
            except TypeError:
                log.warning("Stock code is not available!")
                continue
            log.info(stock_id+":successful!")
            stock_detail.append(stock_id)
            stocks_detail.append(stock_detail)

        columns = ['sprice', 'increase_rt', 'market_value', 'debt_with_interest',
                   'average_dividend_yield_5_years', 'listed_dt', 'total_shares',
                   'average_roe_5_years', 'sales_rt_5_years', 'profit_rt_5_years', 'net_profit_rt_5_years',
                   'stock_id']

        df = pd.DataFrame(stocks_detail, columns=columns)
        df['debt_with_interest'] = df['debt_with_interest'].apply(convert_percent)
        df.to_csv(file_name, encoding='gb2312')
    if stocks_id:
        return df[df['stock_id'].isin(stocks_id)]
    else:
        return df


if __name__ == '__main__':
    get_convertible_list()
    get_stocks_detail()

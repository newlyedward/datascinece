# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime, timedelta

from pathlib import Path

from src.log import LogHandler
from src.data.util.crawler import get_post_text
from src.data.setting import raw_data_dir
from src.data.future.spread import get_future_spreads
from src.data.util import convert_percent

log = LogHandler('future.log')

DCE_COLUMNS = ['open', 'high', 'low', 'close', 'pre_settle', 'settle', 'change1','change2','volume','open_interest','oi_chg','turnover']
DCE_OPTION_COLUMNS = ['open', 'high', 'low', 'close', 'pre_settle', 'settle', 'change1', 'change2', 'delta', 'volume', 'open_interest', 'oi_chg', 'turnover', 'exercise_volume']


def get_dce_hq(date: datetime, code='all', category=0):
    """
    获取大连商品交易所日交易数据 20050104 数据起始日
    url = 'http://www.dce.com.cn//publicweb/quotesdata/dayQuotesCh.html'
    url = 'http://www.dce.com.cn/publicweb/quotesdata/exportDayQuotesChData.html'
    form.data =
        dayQuotes.variety	    : all/y/m
        dayQuotes.trade_type	: 0/1  0:期货 1:期权
        year	    : 2019
        month	    : 2 实际月份-1
        day	        : 14
        exportFlag  : txt excel:xls
    :param code: 商品代码
    :param date: datetime
    :param category: 行情类型, 0期货 或 1期权
    :return pd.DataFrame
    大商所日交易数据(DataFrame):
        symbol        合约代码
        date          日期
        open          开盘价
        high          最高价
        low           最低价
        close         收盘价
        volume        成交量
        open_interest   持仓量
        turnover       成交额
        settle        结算价
        pre_settle    前结算价
        variety       合约类别
        或
   大商所每日期权交易数据
        symbol        合约代码
        date          日期
        open          开盘价
        high          最高价
        low           最低价
        close         收盘价
        pre_settle      前结算价
        settle         结算价
        delta          对冲值
        volume         成交量
        open_interest     持仓量
        oi_change       持仓变化
        turnover        成交额
        implied_volatility 隐含波动率
        exercise_volume   行权量
        variety        合约类别
    或 None(给定日期没有交易数据)

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    url = 'http://www.dce.com.cn/publicweb/quotesdata/exportDayQuotesChData.html'
    form_data = {'dayQuotes.variety': code,
                 'dayQuotes.trade_type': category,
                 'year': date.year,
                 'month': date.month - 1,
                 'day': date.day,
                 'exportFlag': 'txt'}

    text = get_post_text(url, form_data)

    file_path = raw_data_dir / 'future_hq/dce/{}_daily.txt'.format(date.strftime('%Y%m%d'))

    file_path.write_text(text)

    return None
    # dict_data = list()
    # implied_data = list()
    # for idata in data[1:]:
    #     if u'小计' in idata.text or u'总计' in idata.text:
    #         continue
    #     x = idata.find_all('td')
    #     if type == 'future':
    #         row_dict = {'variety': cons.DCE_MAP[x[0].text.strip()]}
    #         row_dict['symbol'] = row_dict['variety'] + x[1].text.strip()
    #         for i, field in enumerate(listed_columns):
    #             field_content = x[i + 2].text.strip()
    #             if '-' in field_content:
    #                 row_dict[field] = 0
    #             elif field in ['volume', 'open_interest']:
    #                 row_dict[field] = int(field_content.replace(',', ''))
    #             else:
    #                 row_dict[field] = float(field_content.replace(',', ''))
    #         dict_data.append(row_dict)
    #     elif len(x) == 16:
    #         m = cons.FUTURE_SYMBOL_PATTERN.match(x[1].text.strip())
    #         if not m:
    #             continue
    #         row_dict = {'symbol': x[1].text.strip(), 'variety': m.group(1).upper(), 'contract_id': m.group(0)}
    #         for i, field in enumerate(listed_columns):
    #             field_content = x[i + 2].text.strip()
    #             if '-' in field_content:
    #                 row_dict[field] = 0
    #             elif field in ['volume', 'open_interest']:
    #                 row_dict[field] = int(field_content.replace(',', ''))
    #             else:
    #                 row_dict[field] = float(field_content.replace(',', ''))
    #         dict_data.append(row_dict)
    #     elif len(x) == 2:
    #         implied_data.append({'contract_id': x[0].text.strip(), 'implied_volatility': float(x[1].text.strip())})
    # df = pd.DataFrame(dict_data)
    # df['date'] = day.strftime('%Y%m%d')
    # if type == 'future':
    #     return df[output_columns]
    # else:
    #     return pd.merge(df, pd.DataFrame(implied_data), on='contract_id', how='left', indicator=False)[output_columns]


if __name__ == '__main__':
    start_dt = datetime(2018, 12, 21)
    end_dt = datetime(2019, 3, 15)
    print(datetime.now())
    get_dce_hq(end_dt)
    # get_future_inventory(start=datetime(2014, 5, 23), end=datetime.today())
    # df = get_future_hq('M', start=start_dt, end=None)
    print(datetime.now())
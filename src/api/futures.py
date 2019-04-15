# -*- coding: utf-8 -*-
import pandas as pd

from datetime import datetime

from src.util import connect_mongo
from log import LogHandler

log = LogHandler('api.log')


def get_dominant(code, start_date=None, end_date=None):
    """
    获取某一期货品种一段时间的主力合约列表。合约首次上市时，以当日收盘同品种持仓量最大者作为从第二个交易日开始的主力合约。
    当同品种其他合约持仓量在收盘后超过当前主力合约时，从第二个交易日开始进行主力合约的切换。日内不会进行主力合约的切换。

    :param code:                            期货合约品种，例如沪深300股指期货为'IF'
    :param start_date:  datetime.datetime   开始日期，默认为期货品种最早上市日期后一交易日
    :param end_date:                        结束日期，默认为当前日期

    :return: pd.DataFrame
    """
    # 连接数据库
    conn = connect_mongo(db='quote')

    cursor = conn['index']

    filter_dict = {'code': code, 'symbol': code+'99'}

    if start_date is not None:  # 使用前一个交易日
        filter_dict['datetime'] = {'$gte': start_date}

    if end_date is not None:
        if 'datetime' in filter_dict:
            filter_dict['datetime']['$lte'] = end_date
        else:
            filter_dict['datetime'] = {'$lte': end_date}

    contract = cursor.find(filter_dict, {'_id': 0, 'datetime': 1, 'contract': 1})

    contract_df = pd.DataFrame(list(contract))

    contract_df.set_index('datetime', inplace=True)

    return contract_df


def get_contracts(code, date=None):
    """
    获取某一期货品种在策略当前日期的可交易合约symbol列表。
    按照到期月份，下标从小到大排列，返回列表中第一个合约对应的就是该品种的近月合约。
    :param code:   期货合约品种，例如沪深300股指期货为'IF'
    :param date:   datetime.datetime 查询日期，默认为当日
    :return: list
    """
    # 连接数据库
    conn = connect_mongo(db='quote')

    cursor = conn['future']

    if date is None:
        date = datetime.today()

    filter_dict = {'code': code, 'datetime': date}

    contract = cursor.find(filter_dict, {'_id': 0, 'symbol': 1})

    return pd.DataFrame(list(contract))


def get_member_rank(symbol, trading_date, rank_by):
    """
    获取期货某合约的会员排名数据
    :param symbol:         可以是期货的具体合约或者品种,合约代码，如CU1903,品种 CU
    :param trading_date:   datetime.datetime 交易日期,默认为当日
    :param rank_by:        排名依据，默认为volume即根据持仓量统计排名，另外可选'long'和'short'，分别对应持买仓量统计和持卖仓量统计。
    :return:
        -pandas DataFrame
            commodity      code/symbol 期货品种代码或期货合约代码
            member_name    期货商名称
            rank           排名
            volume         交易量或持仓量视乎参数rank_by的设定
            volume_change  交易量或持仓量较之前的变动
    """
    pass


def get_warehouse_stocks(code, start_date=None, end_date=None):
    """
    获取期货某品种的注册仓单数据
    :param code:        合约代码，如CU1903
    :param start_date:  开始日期，必须指定
    :param end_date:    结束日期，默认为策略当天日期的前一天
    :return:
        -pandas DataFrame
            on_warrant  注册仓单量
            market      期货品种对应交易所
    """


if __name__ == '__main__':
    start = datetime(2019, 1, 1)
    end = datetime(2006, 8, 3)
    # contracts = get_contracts('CU')
    # contracts = get_contracts('CU', end)
    # df = get_dominant('CU')
    # df = get_dominant('CU', start_date=start)
    # df = get_dominant('CU', start_date=start, end_date=end)
    # df = get_dominant('CU', end_date=start)
    # df = get_price(['CU88', 'M88'], start_date=start, end_date=end, fields=['open', 'close'])


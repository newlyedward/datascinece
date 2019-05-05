import re
import pandas as pd
from datetime import datetime, timedelta
from pymongo import DESCENDING

from src.api import FREQ
from src.analysis import conn

from log import LogHandler

log = LogHandler('analysis.log')


def get_instrument_symbols(by='amount', threshold=1e9, instrument='future'):
    """
    根据流动性选择交易品种，30天内的平均值作为选择标准，由于节假日原因，取平均的数据个数可能不一样。
    :param instrument:
    :param by:过滤的因子
    :param threshold:过滤的阈值
    :return:
    """
    # conn = connect_mongo(db='quote', username=DATA_ANALYST, password=ANALYST_PWD)

    pipeline = [
        {
            '$match': {
                'datetime': {
                    '$gt': datetime.today() - timedelta(30)  # 选取30天内有交易的品种
                }
            }
        }, {
            '$sort': {
                'datetime': DESCENDING
            }
        }, {
            '$group': {
                '_id': '$symbol',
                'datetime': {
                    '$first': '$datetime'
                },
                by: {
                    '$avg': '$' + by
                }
            }
        }, {
            '$match': {
                by: {
                    '$gt': threshold
                }
            }
        }, {
            '$sort': {
                by: DESCENDING
            }
        }
    ]

    match_stage = pipeline[0]['$match']
    cursor = conn['index']
    if instrument == 'future':
        match_stage['symbol'] = {'$regex': re.compile(r"88$")}
    else:
        pass

    symbol_list = list(cursor.aggregate(pipeline))
    # 剔除某一天超过阈值的
    if len(symbol_list) == 0:
        log.info('None of snapshot dates return!')
        return symbol_list
    return [symbol['_id'] for symbol in symbol_list]
    # return symbol_list


def get_peak_start_date(symbol=None, frequency='m', peak_type=None, skip=1):
    """
        返回某个趋势极值点的位置，作为分析的起点
    :param symbol: 合约代码，symbol, symbol list
    :param peak_type:        'down' 底部 'up' 顶部  None 任意极值点
    :param skip:        0 倒数第一个
    :param frequency:   历史数据的频率, 默认为'm', 分析月线的趋势
    :return:
        [{symbol:datetime}]   对应symbol的分析起始时间
    """
    # 连接数据库
    # conn = connect_mongo(db='quote', username=DATA_ANALYST, password=ANALYST_PWD)

    cursor = conn['block']

    pipeline = [
        {
            '$match': {
                'symbol': {
                    '$regex': re.compile(r"88$")
                },
                'frequency': FREQ.index(frequency),
                'sn': 0
            }
        }, {
            '$project': {
                'start_date': 1,
                'symbol': 1
            }
        }, {
            '$sort': {
                'start_date': DESCENDING
            }
        }, {
            '$group': {
                '_id': '$symbol',
                'start_date': {
                    '$push': '$start_date'
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'symbol': "$_id",
                'start_date': {'$arrayElemAt': ['$start_date', 1]}
            }
        }
    ]

    match_stage = pipeline[0]['$match']
    if peak_type is not None:
        match_stage['type'] = peak_type

    if isinstance(symbol, list):
        match_stage['symbol'] = {'$in': symbol}
    elif isinstance(symbol, str):
        match_stage['symbol'] = symbol
    else:
        log.debug('Search all instruments snapshot start datetime!')

    dates = cursor.aggregate(pipeline)

    dates_df = pd.DataFrame(list(dates))
    return dates_df


def get_last_price(symbol=None, instrument='index', frequency='d', fields='close'):
    """
        获取最新的行情数据
    :param symbol: 合约代码，symbol, symbol list, 只支持同种类。获取tick数据时，只支持单个symbol
    :param instrument:   行情数据类型 ['future', 'option', 'stock', 'bond', 'convertible'] 以及 index
    :param frequency:   历史数据的频率, 默认为'd', 只支持日线级别以上数据。'5m'代表5分钟线。可支持期货tick级别数据获取，此时频率为'tick'
    :param fields:      字段名称
    :return:
        传入一个symbol，多个fields，函数会返回一个pandas DataFrame
        传入一个symbol，一个field，函数会返回pandas Series
        传入多个symbol，一个field，函数会返回一个pandas DataFrame
        传入多个symbol，函数会返回一个multiIndex DataFrame
    """
    # 连接数据库
    # conn = connect_mongo(db='quote', username=DATA_ANALYST, password=ANALYST_PWD)

    cursor = conn[instrument]

    pipeline = [
        {
            '$match': {
                'symbol': {
                    '$regex': re.compile(r"88$")
                }
            }
        }, {
            '$sort': {
                'datetime': DESCENDING
            }
        }, {
            '$group': {
                '_id': '$symbol',
                'datetime': {
                    '$first': '$datetime'
                },
                'close': {
                    '$first': '$close'
                },
                'amount': {
                    '$first': '$amount'
                }
            }
        }, {
            '$sort': {
                'amount': DESCENDING
            }
        }
    ]

    match_stage = pipeline[0]['$match']
    if isinstance(symbol, list):
        match_stage['symbol'] = {'$in': symbol}
    elif isinstance(symbol, str):
        match_stage['symbol'] = symbol
    else:
        log.debug('Search all instruments snapshot start datetime!')

    last_prices = cursor.aggregate(pipeline)

    last_prices_df = pd.DataFrame(list(last_prices))
    last_prices_df.rename(columns={'_id': 'symbol'}, inplace=True)
    return last_prices_df


def get_snapshot(symbol=None, instrument='index'):
    if symbol is None:
        symbol = get_instrument_symbols(by='amount')

    start_df = get_peak_start_date(symbol=symbol)

    start_df['start_date'] = start_df['start_date'].fillna(datetime(1970, 1, 1))

    columns = ['code', 'start_date', 'amount', 'dom_contract', 'close', 'highest',
               'lowest', 'percentile', 'wave_rt', 'datetime']

    snapshot_df = pd.DataFrame(columns=columns)

    index_cursor = conn[instrument]

    for row in start_df.itertuples():
        symbol, start_date = row.symbol, row.start_date
        filter_dict = {
            'symbol': row.symbol,
            'datetime': {
                '$gte': row.start_date
            }
        }

        hq = index_cursor.find(filter_dict).sort([('datetime', DESCENDING)])

        hq_df = pd.DataFrame(list(hq))

        last_hq = hq_df.iloc[0]

        close = last_hq['close']
        highest = hq_df['high'].max()
        lowest = hq_df['low'].min()
        wave_rt = (close - lowest) / (highest - lowest)

        # 计算分位数
        x = hq_df[['open', 'high', 'low', 'close']].unstack().sort_values().reset_index()
        y = x[x[0] > close]
        percentile = y.index[0] / len(x)

        hq_related = [last_hq['code'], start_date, last_hq['amount'], last_hq['contract'], close,
                      highest, lowest, percentile, wave_rt, last_hq['datetime']]

        insert_row = pd.DataFrame([hq_related], columns=columns)
        snapshot_df = snapshot_df.append(insert_row, ignore_index=True)

    return snapshot_df


if __name__ == '__main__':
    # symbols = ['A88', 'Y88', 'ME88']
    # symbols = get_instrument_symbols(by='amount')
    # start_dates = get_peak_start_date(symbol=symbols)
    # print(start_dates)
    # last_price_df = get_last_price(symbol=symbols)
    get_snapshot()
    # print(last_price_df)

import re
from datetime import datetime, timedelta

from src.api import FREQ
from src.util import connect_mongo, DATA_ANALYST, ANALYST_PWD

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
    conn = connect_mongo(db='quote', username=DATA_ANALYST, password=ANALYST_PWD)

    pipeline = [
        {
            '$match': {
                'datetime': {
                    '$gt': datetime.today() - timedelta(30)    # 选取30天内有交易的品种
                }
            }
        }, {
            '$sort': {
                'datetime': -1
            }
        }, {
            '$group': {
                '_id': '$symbol',
                'datetime': {
                    '$first': '$datetime'
                },
                by: {
                    '$avg': '$'+by
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
                by: -1
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
    # return [symbol['_id'] for symbol in symbol_list]
    return symbol_list


def get_snapshot_start_date(symbol=None, frequency='m', peak_type=None, skip=1):
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
    conn = connect_mongo(db='quote', username=DATA_ANALYST, password=ANALYST_PWD)

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
                'start_date': -1
            }
        }, {
            '$group': {
                '_id': '$symbol',
                'start_date': {
                    '$push': '$start_date'
                }
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

    dates = list(cursor.aggregate(pipeline))

    if len(dates) == 0:
        log.info('None of snapshot dates return!')
        return dates
    return [dict(symbol=date['_id'], start_date=date['start_date'][skip] if len(date['start_date']) > skip else None)
            for date in dates]


if __name__ == '__main__':
    symbols = ['A88', 'Y88', 'ME88']
    # start_dates = get_snapshot_start_date(symbol=symbols)
    get_instrument_symbols(by='amount')
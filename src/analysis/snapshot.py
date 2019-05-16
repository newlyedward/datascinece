import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pymongo import DESCENDING

from src.api import FREQ
from src.analysis import conn

from log import LogHandler
from src.data.future.setting import CODE2NAME_MAP
from src.util import count_percentile

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


def get_snapshot(symbol=None, instrument='index', threshold=1e9):
    if symbol is None:
        symbol = get_instrument_symbols(by='amount', threshold=threshold)

    start_df = get_peak_start_date(symbol=symbol)

    start_df['start_date'] = start_df['start_date'].fillna(datetime(1970, 1, 1))

    # price status
    columns = ['name', 'percentile', 'wave_rt',
               'amount', 'close', 'highest', 'lowest',
               'contract', 'start_date', 'datetime']

    snapshot_df = pd.DataFrame(columns=columns)

    index_cursor = conn[instrument]

    for row in start_df.itertuples():
        symbol, start_date = row.symbol, row.start_date
        filter_dict = {
            'symbol': symbol,
            'datetime': {
                '$gte': start_date
            }
        }

        hq = index_cursor.find(filter_dict).sort([('datetime', DESCENDING)])

        hq_df = pd.DataFrame(list(hq))

        hq_df = hq_df[hq_df['low'] > 0]  # 历史成交量可能为0，没有成交价格

        last_hq = hq_df.iloc[0]

        code = last_hq['code']
        close = last_hq['close']
        highest = hq_df['high'].max()
        lowest = hq_df['low'].min()
        snapshot_df.loc[code, 'wave_rt'] = (close - lowest) / (highest - lowest)

        # 计算分位数 TODO 使用函数计算
        x = hq_df[['open', 'high', 'low', 'close']].unstack().sort_values().reset_index()
        y = x[x[0] > close]
        snapshot_df.loc[code, 'percentile'] = y.index[0] / len(x)

        # columns = ['code', 'name', 'percentile', 'wave_rt',
        #            'amount', 'close', 'highest', 'lowest',
        #            'dom_contract', 'start_date', 'datetime']
        snapshot_df.loc[code, 'name'] = CODE2NAME_MAP.get(code, code)
        snapshot_df.loc[code, 'amount'] = last_hq['amount'] / 1e8
        snapshot_df.loc[code, 'close'] = close
        snapshot_df.loc[code, 'highest'] = highest
        snapshot_df.loc[code, 'lowest'] = lowest
        snapshot_df.loc[code, 'contract'] = last_hq['contract']
        snapshot_df.loc[code, 'start_date'] = start_date
        snapshot_df.loc[code, 'datetime'] = last_hq['datetime']

    snapshot_df.sort_values(by='percentile', inplace=True)

    # roll yield status
    columns = ['deliver_yield', 'nearby_yield', 'far_month_yield',
               'deliver_yield_pct', 'nearby_yield_pct', 'far_month_yield_pct']
    roll_yield_df = pd.DataFrame(index=snapshot_df.index, columns=columns)

    for code in roll_yield_df.index:
        start_date = snapshot_df.loc[code, 'start_date']

        filter_dict = {
            'symbol': {"$regex": "^"+code+"[7-9]{2}$"},
            'datetime': {
                '$gte': start_date
            }
        }

        projection = {
            "_id": 0,
            "symbol": 1,
            "datetime": 1,
            "close": 1
        }

        hq = index_cursor.find(filter_dict, projection=projection)

        hq_df = pd.DataFrame(list(hq))
        hq_df = hq_df.pivot(index='datetime', columns='symbol', values='close')

        spot_cursor = conn['spot_price']
        filter_dict = {"code": code}
        projection = {"_id": 0, "datetime": 1, "spot": 1}
        spot = spot_cursor.find(filter_dict, projection=projection)
        spot_df = pd.DataFrame(list(spot))
        if spot_df.empty:
            yield_df = hq_df
            yield_df.columns = ['deliver', 'domain', 'far_month']
            yield_df['deliver_yield'] = np.nan
        else:
            spot_df.set_index('datetime', inplace=True)
            yield_df = pd.concat([spot_df, hq_df], axis=1)
            yield_df = yield_df.dropna()
            yield_df.columns = ['spot', 'deliver', 'domain', 'far_month']
            yield_df['deliver_yield'] = (yield_df['deliver'] / yield_df['spot'] - 1) * 100

        yield_df['nearby_yield'] = (yield_df['domain'] / yield_df['deliver'] - 1) * 100
        yield_df['far_month_yield'] = (yield_df['far_month'] / yield_df['domain'] - 1) * 100

        last_yield = yield_df.iloc[-1]

        roll_yield_df.loc[code, 'deliver_yield'] = last_yield['deliver_yield']
        roll_yield_df.loc[code, 'nearby_yield'] = last_yield['nearby_yield']
        roll_yield_df.loc[code, 'far_month_yield'] = last_yield['far_month_yield']

        if spot_df.empty:
            roll_yield_df.loc[code, 'deliver_yield_pct'] = np.nan
        else:
            roll_yield_df.loc[code, 'deliver_yield_pct'] = \
                count_percentile(last_yield['deliver_yield'], yield_df['deliver_yield'])

        roll_yield_df.loc[code, 'nearby_yield_pct'] = \
            count_percentile(last_yield['nearby_yield'],
                             yield_df.loc[yield_df['nearby_yield'] != 0, 'nearby_yield'])
        roll_yield_df.loc[code, 'far_month_yield_pct'] = \
            count_percentile(last_yield['far_month_yield'],
                             yield_df.loc[yield_df['far_month_yield'] != 0, 'far_month_yield'])

    return snapshot_df, roll_yield_df


if __name__ == '__main__':
    # symbols = ['A88', 'Y88', 'ME88']
    # symbols = get_instrument_symbols(by='amount')
    # start_dates = get_peak_start_date(symbol=symbols)
    # print(start_dates)
    # last_price_df = get_last_price(symbol=symbols)
    snapshot_df, block_status_df, roll_yield_df = get_snapshot(threshold=1e11)
    with pd.ExcelWriter('output.xlsx') as writer:
        snapshot_df.to_excel(writer, sheet_name='snapshot')
        block_status_df.to_excel(writer, sheet_name='block_status')
        roll_yield_df.to_excel(writer, sheet_name='roll_yield')
    # print(last_price_df)

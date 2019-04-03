# -*- coding: utf-8 -*-
import json
import re
from datetime import datetime

import numpy as np
import pandas as pd

from src.data.future.setting import HQ_COLUMNS_PATH, CODE2NAME_TABLE
from src.data.future.utils import get_download_file_index, get_insert_mongo_files
from src.data.setting import RAW_HQ_DIR, INSTRUMENT_TYPE
from src.data.util import get_post_text, get_html_text, connect_mongo
from log import LogHandler

# TIME_WAITING = 1
log = LogHandler('future.hq.log')


def is_data_empty(data):
    """
    判断数据是否存在
    :param data: pd.DataFrame or str
    :return: True 数据不存在
    """
    if isinstance(data, pd.DataFrame):
        return data.empty
    elif not isinstance(data, str):
        return True
    elif re.search('doctype', data, re.I):
        return True
    elif len(data) < 100:
        return True
    else:
        return False


def download_cffex_hq_by_date(date: datetime, category=0):
    """
    获取中国金融期货交易所交易所日交易数据 datetime(2010, 4, 30)
    http://www.cffex.com.cn/sj/hqsj/rtj/201903/13/20190313_1.csv
    没有期权，预留接口

    :param date: datetime
    :param category: 行情类型, 0期货 或 1期权
    :return str

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    url_template = 'http://www.cffex.com.cn/fzjy/mrhq/{}/{}/{}_1.csv'
    url = url_template.format(date.strftime('%Y%m'), date.strftime('%d'), date.strftime('%Y%m%d'))

    return get_html_text(url)


def download_czce_hq_by_date(date: datetime, category=0):
    """
    获取郑州商品交易所日交易数据
    http://www.czce.com.cn/cn/DFSStaticFiles/Future/2019/20190314/FutureDataDaily.txt
    http://www.czce.com.cn/cn/DFSStaticFiles/Future/2019/20190314/FutureDataDaily.htm
    期权 datetime(2017, 4, 19)
    http://www.czce.com.cn/cn/DFSStaticFiles/Option/2018/20180816/OptionDataDaily.htm
    http://www.czce.com.cn/cn/DFSStaticFiles/Option/2017/20171109/OptionDataDaily.htm
    datetime(2015, 10, 8)
    http://www.czce.com.cn/cn/exchange/2015/datadaily/20150821.htm
    http://www.czce.com.cn/cn/exchange/2015/datadaily/20150930.txt
    datetime(2010, 8, 24)
    http://www.czce.com.cn/cn/exchange/jyxx/hq/hq20100806.html
    datetime(2005, 4, 29)

    :param date: datetime
    :param category: 行情类型, 0期货 或 1期权
    :return pd.DataFrame

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    index = 0
    ret = pd.DataFrame()

    if date < datetime(2005, 4, 29):
        return pd.DataFrame()
    elif date < datetime(2010, 8, 24):
        url_template = 'http://www.czce.com.cn/cn/exchange/jyxx/hq/hq{}.html'
        url = url_template.format(date.strftime('%Y%m%d'))
        index = 1
    elif date < datetime(2015, 10, 8):
        url_template = 'http://www.czce.com.cn/cn/exchange/{}/datadaily/{}.htm'
        url = url_template.format(date.year, date.strftime('%Y%m%d'))
        index = 3
    else:
        template = ['http://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/{}/FutureDataDaily.htm',
                    'http://www.czce.com.cn/cn/DFSStaticFiles/Option/{}/{}/OptionDataDaily.htm']
        url_template = template[category]
        url = url_template.format(date.year, date.strftime('%Y%m%d'))

    text = get_html_text(url)

    if is_data_empty(text):
        return ret

    tables = pd.read_html(text, header=0)

    df = tables[index]

    bflag = df.empty or len(df.columns) < 10 or len(df.columns) > 20

    if not bflag:
        return df

    # 处理特殊的例外情况 2017-12-27 index=3
    for df in tables:
        bflag = df.empty or len(df.columns) < 10 or len(df.columns) > 20
        if not bflag:
            return df

    return ret


def download_shfe_hq_by_date(date: datetime, category=0):
    """
    获取上海商品交易所日交易数据 20020108/20090105 期货数据起始日（还可以往前取） 2018921 期权数据起始日
    http://www.shfe.com.cn/data/dailydata/kx/kx20190318.dat
    http://www.shfe.com.cn/data/dailydata/option/kx/kx20190315.dat
    :param date: datetime
    :param category: 行情类型, 0期货 或 1期权
    :return str

    """
    assert date <= datetime.today()
    assert category in [0, 1]

    url_template = ['http://www.shfe.com.cn/data/dailydata/kx/kx{}.dat',
                    'http://www.shfe.com.cn/data/dailydata/option/kx/kx{}.dat']
    url = url_template[category].format(date.strftime('%Y%m%d'))

    return get_html_text(url)


def download_dce_hq_by_date(date: datetime, code='all', category=0):
    """
    获取大连商品交易所日交易数据 20050104 期货数据起始日 2017331 期权数据起始日
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
    :return str

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
    return get_post_text(url, form_data)


def download_hq_by_date(date, file_path, market='dce', category=0):
    """
    从交易所网站获取某天的所有行情数据，存盘并返回pd.DataFrame
    :param date: 需要数据的日期
    :param file_path: 存储文件的地址
    :param market: 交易所代码
    :param category: 0:期货 1：期权
    :return: pd.DataFrame
    """
    assert category in [0, 1]
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    get_exchange_hq_func = {'cffex': download_cffex_hq_by_date,
                            'czce': download_czce_hq_by_date,
                            'shfe': download_shfe_hq_by_date,
                            'dce': download_dce_hq_by_date}

    data = get_exchange_hq_func[market](date, category=category)
    date_str = date.strftime('%Y%m%d')

    if is_data_empty(data):
        log.warning('{} {} data:{} is not downloaded! '.format(market, date_str, category))
        # time.sleep(np.random.rand() * TIME_WAITING * 3)
        return False

    if market == 'czce':
        data.to_csv(file_path, encoding='gb2312')
    else:
        file_path.write_text(data)

    return True


def download_hq_by_dates(market, category=0):
    """
    根据日期连续下载交易所日交易数据
    :param market:
    :param category: 行情类型, 0期货 或 1期权
    :return True False: 说明不用下载数据

    """
    assert category in [0, 1]
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    target = RAW_HQ_DIR[category] / market

    file_index = get_download_file_index(market, category)

    if file_index.empty:
        return False

    for dt in file_index:
        print('{} downloading {} {} hq:{} data!'.format(
            datetime.now().strftime('%H:%M:%S'), market, dt.strftime('%Y-%m-%d'), category))
        date_str = dt.strftime('%Y%m%d')
        file_path = target / '{}.day'.format(date_str)
        download_hq_by_date(dt, file_path, market, category)
        # time.sleep(np.random.rand() * TIME_WAITING)
    return True


def split_symbol(pattern, df):
    """
    对合约代码分析，并用正则表达式进行提取
    :param pattern: 正则表达式
    :param df: 行情数据，包含'symbol columns'
    :return:
        pd.Series, idx 提取出信息对应的索引bool值
    """
    split_s = df['symbol'].transform(lambda x: re.search(pattern, x))
    idx = [True]
    if split_s.isna().any():
        idx = ~split_s.isna().values
        split_s = split_s.dropna()
        log.warning(
            "There are some Nan in re search!")
    return split_s, idx


def transfer_exchange_data(file_row, market='dce', category=0):
    """
    将每天的数据统一标准
    :param category: o 期货 1 期权
    :param market: 交易市场缩写
    :param file_row: collections.namedtuple,  a namedtuple for each row in the DataFrame
    :return: pd.DataFrame 统一标准后的数据
    """
    assert category in [0, 1]
    assert market in ['dce', 'czce', 'shfe', 'cffex']

    ret = pd.DataFrame()

    file_path = file_row.filepath
    date = file_row.Index

    cols_path = HQ_COLUMNS_PATH[category]

    cols_df = pd.read_csv(cols_path, index_col=False, header=0,
                          usecols=['columns', market], encoding='gb2312')
    cols_df = cols_df.dropna()
    columns = cols_df[market].values  # 需要读取的数据列
    names = cols_df['columns'].values  # 对应的统一后的列的名称

    # dtype = dict(cols_df[[market, 'dtype']].values)  # 每列对应转换的数据类型
    if market == 'czce' and date < datetime(2010, 8, 24) and category == 0:
        cols_df.loc[cols_df['columns'] == 'volume', 'czce'] = '成交量'

    # 读取需转换的原始数据文件
    if market == 'dce':  # text
        hq_df = pd.read_csv(file_path, encoding='gb2312', header=0, index_col=False,
                            sep='\s+', thousands=',')
    elif market == 'shfe':  # json
        hq_df = pd.DataFrame(json.loads(file_path.read_text())['o_curinstrument'])
    else:
        hq_df = pd.read_csv(file_path, encoding='gb2312', header=0, index_col=False)

    bflag = hq_df.empty or len(hq_df.columns) < len(columns) or len(hq_df.columns) > 20
    if bflag:  # 原始数据文件为null，不重新下载，需要再运行一次程序
        print('{} instrument type:{} {} hq data is not exist, please rerun program!'.
              format(market, category, file_row.filepath.name))
        return ret

    # 截取所需字段，并将字段名转换为统一标准
    hq_df = hq_df[columns]
    hq_df.columns = names
    hq_df = hq_df.dropna()

    # 处理上海市场的空格和多余行
    if market == 'shfe':
        hq_df = hq_df[hq_df['settle'] != '']
        if category == 0:
            hq_df.loc[:, 'commodity'] = hq_df['commodity'].str.strip()
        else:
            hq_df.loc[:, 'symbol'] = hq_df['symbol'].str.strip()

    # 商品字母缩写转换和合约代码组织
    if category == 0:
        if market in ['shfe', 'dce']:
            code2name_table = CODE2NAME_TABLE.loc[CODE2NAME_TABLE['market'] == market, ['code', 'exchange']]
            code2name_table.set_index('exchange', inplace=True)
            hq_df = hq_df.join(code2name_table, on='commodity')
            del hq_df['commodity']
        elif market in ['czce', 'cffex']:
            split_re, index = split_symbol('^[a-zA-Z]{1,2}', hq_df)
            hq_df = hq_df if all(index) else hq_df[index]
            hq_df['code'] = split_re.transform(lambda x: x[0])
        else:
            log.info('Wrong exchange market name!')
            return ret

        # 提取交割月份
        def convert_deliver(x):
            if not isinstance(x, str):
                x = str(x).strip()
            m = re.search('\d{3,4}$', x.strip())[0]
            if len(m) == 4:
                return m

            if m[0] == '0':
                y = date.year
                y = int(y - np.floor(y / 100) * 100 + 1)
                m = str(y) + m[-2:]
            else:
                m = date.strftime('%y')[0] + m

            return m

        if market in ['dce', 'czce', 'shfe']:  # cffex symbol不需要转换
            hq_df['symbol'] = hq_df['code'] + hq_df['symbol'].transform(convert_deliver)

    else:  # category = 1 期权
        hq_df.loc[:, 'symbol'] = hq_df['symbol'].str.upper() \
            .str.replace('-', '').str.strip()

        split_re, index = split_symbol('^([A-Z]{1,2})(\d{3,4})-?([A-Z])-?(\d+)', hq_df)
        hq_df = hq_df if all(index) else hq_df[index]

        hq_df['code'] = split_re.transform(lambda x: x[1])
        hq_df['future_symbol'] = split_re.transform(
            lambda x: x[1] + (x[2] if len(x[2]) == 4 else '0{}'.format(x[2])))
        hq_df['type'] = split_re.transform(lambda x: x[3])
        hq_df['exeprice'] = pd.to_numeric(split_re.transform(lambda x: x[4]), downcast='float')

    # 数据类型转换，避免合并时出现错误
    hq_df['open'] = pd.to_numeric(hq_df['open'], downcast='float')
    hq_df['close'] = pd.to_numeric(hq_df['close'], downcast='float')
    hq_df['high'] = pd.to_numeric(hq_df['high'], downcast='float')
    hq_df['low'] = pd.to_numeric(hq_df['low'], downcast='float')
    hq_df['settle'] = pd.to_numeric(hq_df['settle'], downcast='float')

    hq_df['volume'] = pd.to_numeric(hq_df['volume'], downcast='integer')
    hq_df['openInt'] = pd.to_numeric(hq_df['openInt'], downcast='integer')

    # 行权量和delta只有期权独有
    if category == 1:
        hq_df['exevolume'] = pd.to_numeric(hq_df['exevolume'], downcast='integer')
        hq_df['delta'] = pd.to_numeric(hq_df['delta'], downcast='float')

    # 计算成交金额
    if market == 'shfe' and category == 0:  # 这样计算的不准确，只能近似
        hq_df['amount'] = pd.to_numeric(hq_df['volume'] * hq_df['close'], downcast='float')
    else:
        hq_df['amount'] = pd.to_numeric(hq_df['amount'], downcast='float') * 10000
    hq_df['datetime'] = date
    hq_df['market'] = market
    return hq_df


def insert_hq_to_mongo():
    """
    下载数据文件，插入mongo数据库
    :return:
    """
    category = [0, 1]
    market = ['dce', 'czce', 'shfe', 'cffex']

    conn = connect_mongo(db='quote')

    for c in category:
        t = INSTRUMENT_TYPE[c]
        cursor = conn[t]
        for m in market:
            if m == 'cffex' and c == 1:
                print("cffex has no option trading.")
                break
            # 下载更新行情的原始数据
            download_hq_by_dates(m, c)

            # 需要导入数据库的原始数据文件
            file_df = get_insert_mongo_files(m, c)

            if file_df.empty:
                print('{} {} hq is updated before!'.format(m, t))
                continue

            for row in file_df.itertuples():
                df = transfer_exchange_data(row, m, c)
                if df.empty:
                    log.error("Transform {} {} {}data failure, please check program.".format(m, t, row.Index))
                    continue
                result = cursor.insert_many(df.to_dict('records'))
                if result:
                    print('{} {} {} insert success.'.format(m, t, row.filepath.name))
                else:
                    print('{} {} {} insert failure.'.format(m, t, row.filepath.name))
            print('{} {} hq is updated now!'.format(m, t))


def build_weighted_index(hq_df, weight='volume'):
    """
    对行情数据求加权指数
    :param hq_df: pd.MultiIndex(datetime symbol)
    :param weight: str 权重指标 'volume', 'openInt'
    :return: hq_df 剔除了symbol字段或者索引
    """
    df = hq_df.copy()
    columns = ['open', 'high', 'low', 'close']

    for column in columns:
        df[column] = df[column] * df[weight]

    df = df.sum(level=0)

    for column in columns:
        df[column] = df[column] / df[weight]

    return df


def build_future_index():
    """
    编制指数数据：期货加权指数，主力合约指数，远月主力合约，交割主力合约
    对应的symbol-xx00:持仓量加权，xx11：成交量加权，xx88：主力合约，x99：远月合约，xx77:交割月合约
    按成交量对同一天的交易合约进行排序，取排名前三的交易合约，成交量最大的为主力合约
    最接近当月的合约为交割主力合约，在主力合约后交割的为远月主力合约
    :return:
    """
    # 更新数据库行情数据
    insert_hq_to_mongo()

    # 连接数据库
    conn = connect_mongo(db='quote')

    index_cursor = conn['index']
    hq_cursor = conn['future']

    # 从 future collection中提取交易的品种
    codes = hq_cursor.distinct('code')
    if not isinstance(codes, list) or len(codes) == 0:
        print("Don't find any trading code in future collection!")
        return

    # 按品种分别编制指数
    for code in codes:
        # 获取指数数据最近的一条主力合约记录，判断依据是前一天的持仓量
        last_doc = index_cursor.find_one({'symbol': code+'88'}, sort=[('datetime', -1)])

        if last_doc:
            filter_dict = {'code': code, 'datetime': {'$gte': last_doc['datetime']}}
            # 已经改名交易品种['GN', 'WS', 'WT', 'RO', 'ER', 'ME', 'TC']
            #       老合约     新合约      老合约最后交易日
            # 甲醇   ME/50吨   MA/10吨       2015-5-15
            # 动力煤 TC/200吨  ZC/100吨      2016-4-8
            # 强筋小麦 WS/10吨  WH/20吨      2013-05-23
            # 硬白小麦 WT/10吨  PM/50吨      2012-11-22
            # 早籼稻  ER/10吨   RI/20吨      2013-5-23
            # 绿豆    GN                    2010-3-23
            # 菜籽油   RO/5吨   OI/10吨      2013-5-15
            if code in ['GN', 'WS', 'WT', 'RO', 'ER', 'ME', 'TC']:
                print('{} is the {} last trading day.'.format(last_doc['datetime'].strftime('%Y-%m-%d'), code))
                continue
            else:
                print("Build {} future index from {}".format(code, last_doc['datetime']))
        else:  # 测试指定日期
            # filter_dict = {'code': code, 'datetime': {'$lte': datetime(2003, 1, 1)}}
            filter_dict = {'code': code}
            print("Build {} future index from trade beginning.".format(code))

        # 从数据库读取所需数据
        hq = hq_cursor.find(filter_dict, {'_id': 0})
        hq_df = pd.DataFrame(list(hq))
        if hq_df.empty:
            print('{} index data have been updated before!'.format(code))
            continue

        hq_df.set_index(['datetime', 'symbol'], inplace=True)

        date_index = hq_df.index.levels[0]
        if len(date_index) < 2:   # 新的数据
            print('{} index data have been updated before!'.format(code))
            continue

        index_names = ['domain', 'near', 'next']
        contract_df = pd.DataFrame(index=date_index, columns=index_names)

        for date in date_index:
            # hq.py:493: PerformanceWarning: indexing past lexsort depth may impact performance.
            #   s = hq_df.loc[date, 'openInt'].copy()
            s = hq_df.loc[date, 'openInt'].copy()
            s.sort_values(ascending=False, inplace=True)
            s = s[:min(3, len(s))]          # 预防合约小于3的情况,避免出现交割月和主力合约重合，主力合约和下月合约重合
            domain = s.index[0]
            contract_df.loc[date, 'domain'] = domain
            s.sort_index(inplace=True)
            contract_df.loc[date, 'near'] = s.index[0]
            idx = s.index.get_loc(domain) + 1
            if idx >= len(s):     # 远月合约持仓较少，不会存在换月情况，只会和交割月合约交换
                log.debug('{}:{} Far month openInt is few.'.format(code, date.strftime('%Y-%m-%d')))
                idx = len(s) - 1
            contract_df.loc[date, 'next'] = s.index[idx]

        pre_contract_df = contract_df.shift(1).dropna()
        # length = len(contract_df)
        pre_no_index_df = pre_contract_df.reset_index()
        hq_df = hq_df.loc[pre_no_index_df.datetime[0]:]  # 期货指数数据从第二个交易日开始

        frames = []

        index_symbol = [code + x for x in ['00', '11', '88', '77', '99']]
        multi_index_names = ['datetime', 'symbol']
        # 主力，交割，远月合约数据
        for name, symbol in zip(index_names, index_symbol[-3:]):

            multi_index = pd.MultiIndex.from_frame(
                pre_no_index_df[['datetime', name]], names=multi_index_names)
            index_diff = multi_index.difference(hq_df.index)

            # 头一天还有交割仓位，第二天合约消失的情况
            if not index_diff.empty:
                date_index = index_diff.get_level_values(level=0)
                pre_contract_df.loc[date_index, name] = contract_df.loc[date_index, name]
                pre_no_index_df = pre_contract_df.reset_index()
                multi_index = pd.MultiIndex.from_frame(
                    pre_no_index_df[['datetime', name]], names=multi_index_names)
                print('{} use {} current day contract'.format(symbol, len(index_diff)))

            index_df = hq_df.loc[multi_index]
            index_df.reset_index(inplace=True)
            index_df['contract'] = index_df['symbol']
            index_df['symbol'] = symbol
            frames += index_df.to_dict('records')

        # 加权指数
        for symbol, weight_name in zip(index_symbol[:2], ['openInt', 'volume']):
            index_df = build_weighted_index(hq_df, weight=weight_name)
            index_df.reset_index(inplace=True)
            index_df['code'] = code
            index_df['market'] = hq_df.market[0]
            index_df['symbol'] = symbol
            frames += index_df.to_dict('records')

        result = index_cursor.insert_many(frames)
        if result.acknowledged:
            print('{} index data insert success.'.format(code))
        else:
            print('{} index data insert failure.'.format(code))


if __name__ == '__main__':
    # start_dt = datetime(2014, 12, 21)
    # end_dt = datetime(2005, 1, 4)
    print(datetime.now())
    # download_czce_hq_by_date(end_dt)
    # get_dce_hq_by_dates(category=0)
    # download_hq_by_dates('cffex', category=0)
    # get_cffex_hq_by_dates(category=0)
    # construct_dce_hq(end=end_dt, category=0)
    # df = pd.read_csv(file_path, encoding='gb2312', sep='\s+')
    # df = get_future_hq('M', start=start_dt, end=None)

    # date = datetime(2018, 10, 25)
    # filepath = Path(
    #     r'D:\Code\test\cookiercutter\datascience\datascinece\data\raw\future_option\shfe\{}_daily.txt'
    #         .format(date.strftime('%Y%m%d')))
    # Pandas = namedtuple('Pandas', 'Index filepath')
    # row = Pandas(Index=date, filepath=filepath)
    # df = transfer_exchange_data(row, market='shfe', category=1)
    # result = to_mongo('quote', 'option', df.to_dict('records'))
    # insert_hq_to_mongo()
    build_future_index()
    print(datetime.now())

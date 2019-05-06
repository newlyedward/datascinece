import pandas as pd
from src.data.setting import RAW_DATA_DIR, SRC_DATA_FUTURE

SPREAD_DIR = RAW_DATA_DIR / 'future/spread'
INVENTORY_DIR = RAW_DATA_DIR / 'future/inventory'
RECEIPT_DIR = RAW_DATA_DIR / 'future/receipt'

# 商品中文名和字母缩写对照表

NAME2CODE_MAP = {
    "exchange":
        {
            "大豆": "A",
            "豆一": "A",
            "豆二": "B",
            "胶合板": "BB",
            "玉米": "C",
            "玉米淀粉": "CS",
            "乙二醇": "EG",
            "纤维板": "FB",
            "铁矿石": "I",
            "焦炭": "J",
            "鸡蛋": "JD",
            "焦煤": "JM",
            "聚乙烯": "L",
            "豆粕": "M",
            "棕榈油": "P",
            "聚丙烯": "PP",
            "聚氯乙烯": "V",
            "豆油": "Y",
            "白银": "AG",
            "铝": "AL",
            "黄金": "AU",
            "石油沥青": "BU",
            "铜": "CU",
            "燃料油": "FU",
            "热轧卷板": "HC",
            "镍": "NI",
            "铅": "PB",
            "螺纹钢": "RB",
            "天然橡胶": "RU",
            "原油": "SC",
            "锡": "SN",
            "纸浆": "SP",
            "线材": "WR",
            "锌": "ZN",
            "苹果": "AP",
            "棉花": "CF",
            "棉纱": "CY",
            "玻璃": "FG",
            "粳稻": "JR",
            "晚籼稻": "LR",
            "甲醇": "MA",
            "菜籽油": "OI",
            "普麦": "PM",
            "早籼稻": "RI",
            "菜籽粕": "RM",
            "油菜籽": "RS",
            "硅铁": "SF",
            "锰硅": "SM",
            "白糖": "SR",
            "PTA": "TA",
            "强麦": "WH",
            "动力煤": "ZC",
            # --------czce商品简称曾用名-------
            "ME": "MA",
            "TC": "ZC",
            "WS": "WH",
            "WT": "PM",
            "ER": "RI",
            "RO": "OI",
            "绿豆": "GN"
            # 已经改名交易品种['GN', 'WS', 'WT', 'RO', 'ER', 'ME', 'TC']
            #       老合约     新合约      老合约最后交易日
            # 甲醇   ME/50吨   MA/10吨       2015-5-15
            # 动力煤 TC/200吨  ZC/100吨      2016-4-8
            # 强筋小麦 WS/10吨  WH/20吨      2013-05-23
            # 硬白小麦 WT/10吨  PM/50吨      2012-11-22
            # 早籼稻  ER/10吨   RI/20吨      2013-5-23
            # 绿豆    GN                    2010-3-23
            # 菜籽油   RO/5吨   OI/10吨      2013-5-15
        },
    "spread":
        {
            "豆一": "A",
            "玉米": "C",
            "玉米淀粉": "CS",
            "铁矿石": "I",
            "焦炭": "J",
            "鸡蛋": "JD",
            "焦煤": "JM",
            "聚乙烯": "L",
            "豆粕": "M",
            "棕榈油": "P",
            "聚丙烯": "PP",
            "聚氯乙烯": "V",
            "豆油": "Y",
            "白银": "AG",
            "铝": "AL",
            "黄金": "AU",
            "石油沥青": "BU",
            "铜": "CU",
            "热轧卷板": "HC",
            "镍": "NI",
            "铅": "PB",
            "螺纹钢": "RB",
            "天然橡胶": "RU",
            "锡": "SN",
            "纸浆": "SP",
            "线材": "WR",
            "锌": "ZN",
            "棉花": "CF",
            "棉纱": "CY",
            "玻璃": "FG",
            "甲醇MA": "MA",
            "菜籽油OI": "OI",
            "普麦": "PM",
            "菜籽粕": "RM",
            "油菜籽": "RS",
            "硅铁": "SF",
            "锰硅": "SM",
            "白糖": "SR",
            "PTA": "TA",
            "硬麦": "WH",
            "动力煤ZC": "ZC",
            "动力煤": "ZC",
        }

}

CODE2NAME_MAP = {
    "A": "豆一",
    "B": "豆二",
    "BB": "胶合板",
    "C": "玉米",
    "CS": "玉米淀粉",
    "EG": "乙二醇",
    "FB": "纤维板",
    "I": "铁矿石",
    "J": "焦炭",
    "JD": "鸡蛋",
    "JM": "焦煤",
    "L": "聚乙烯",
    "M": "豆粕",
    "P": "棕榈油",
    "PP": "聚丙烯",
    "V": "聚氯乙烯",
    "Y": "豆油",
    "AG": "白银",
    "AL": "铝",
    "AU": "黄金",
    "BU": "沥青",
    "CU": "铜",
    "FU": "燃料油",
    "HC": "热轧卷板",
    "NI": "镍",
    "PB": "铅",
    "RB": "螺纹钢",
    "RU": "天然橡胶",
    "SC": "原油",
    "SN": "锡",
    "SP": "纸浆",
    "WR": "线材",
    "ZN": "锌",
    "AP": "苹果",
    "CF": "棉花",
    "CY": "棉纱",
    "FG": "玻璃",
    "JR": "粳稻",
    "LR": "晚籼稻",
    "MA": "甲醇",
    "OI": "菜籽油",
    "PM": "普麦",
    "RI": "早籼稻",
    "RM": "菜籽粕",
    "RS": "油菜籽",
    "SF": "硅铁",
    "SM": "锰硅",
    "SR": "白糖",
    "TA": "PTA",
    "WH": "强麦",
    "ZC": "动力煤"
}

# 下载文件字段名和数据库字段名的映射
COLUMNS_MAP = {
    "future":
        {
            "czce":
                {
                    "symbol": "品种月份",
                    "open": "今开盘",
                    "high": "最高价",
                    "low": "最低价",
                    "close": "今收盘",
                    "volume": ["成交量", "成交量(手)"],
                    "openInt": "空盘量",
                    "amount": "成交额(万元)",
                    "settle": "今结算"
                },
            "dce":
                {
                    "code": "商品名称",
                    "symbol": "交割月份",
                    "open": "开盘价",
                    "high": "最高价",
                    "low": "最低价",
                    "close": "收盘价",
                    "volume": "成交量",
                    "openInt": "持仓量",
                    "amount": "成交额",
                    "settle": "结算价"
                },
            "shfe":
                {
                    "code": "PRODUCTNAME",
                    "symbol": "DELIVERYMONTH",
                    "open": "OPENPRICE",
                    "high": "HIGHESTPRICE",
                    "low": "LOWESTPRICE",
                    "close": "CLOSEPRICE",
                    "volume": "VOLUME",
                    "openInt": "OPENINTEREST",
                    "settle": "SETTLEMENTPRICE"
                }
            ,
            "cffex":
                {
                    "symbol": "合约代码",
                    "open": "今开盘",
                    "high": "最高价",
                    "low": "最低价",
                    "close": "今收盘",
                    "volume": "成交量",
                    "openInt": "持仓量",
                    "amount": "成交金额",
                    "settle": "今结算"
                }
        },
    "option":
        {
            "czce":
                {
                    "symbol": "品种代码",
                    "open": "今开盘",
                    "high": "最高价",
                    "low": "最低价",
                    "close": "今收盘",
                    "volume": "成交量(手)",
                    "openInt": "空盘量",
                    "amount": "成交额(万元)",
                    "settle": "今结算",
                    "exevolume": "行权量",
                    "delta": "DELTA",
                    "sigma": "隐含波动率"
                },
            "dce":
                {
                    # "commodity": "商品名称",
                    "symbol": "合约名称",
                    "open": "开盘价",
                    "high": "最高价",
                    "low": "最低价",
                    "close": "收盘价",
                    "volume": "成交量",
                    "openInt": "持仓量",
                    "amount": "成交额",
                    "settle": "结算价",
                    "exevolume": "行权量",
                    "delta": "Delta",
                },
            "shfe":
                {
                    # "commodity": "PRODUCTNAME",
                    "symbol": "INSTRUMENTID",
                    "open": "OPENPRICE",
                    "high": "HIGHESTPRICE",
                    "low": "LOWESTPRICE",
                    "close": "CLOSEPRICE",
                    "volume": "VOLUME",
                    "openInt": "OPENINTEREST",
                    "amount": "TURNOVER",
                    "settle": "SETTLEMENTPRICE",
                    "exevolume": "EXECVOLUME",
                    "delta": "DELTA",
                    # "sigma": "SIGMA"
                }
        }
}

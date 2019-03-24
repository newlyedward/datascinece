import pandas as pd
from src.data.setting import RAW_DATA_DIR, PROCESSED_DATA_DIR, SRC_DATA_FUTURE

SPREAD_DIR = RAW_DATA_DIR / 'future/spread'
INVENTORY_DIR = RAW_DATA_DIR / 'future/inventory'
RECEIPT_DIR = RAW_DATA_DIR / 'future/receipt'

HQ_COLUMNS_PATH = [SRC_DATA_FUTURE / 'columns_future.csv', SRC_DATA_FUTURE / 'columns_option.csv']

# 商品中文名和字母缩写对照表
CODE2NAME_PATH = SRC_DATA_FUTURE / 'code2name.csv'
CODE2NAME_TABLE = pd.read_csv(CODE2NAME_PATH, encoding='gb2312', header=0,
                              usecols=['code', 'market', 'exchange']).dropna()



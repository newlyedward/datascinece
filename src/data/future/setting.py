import pandas as pd
from src.data.setting import RAW_DATA_DIR, PROCESSED_DATA_DIR, SRC_DATA_FUTURE

PROCESSED_HQ_DIR = [PROCESSED_DATA_DIR / 'future_hq', PROCESSED_DATA_DIR / 'future_option']
RAW_HQ_DIR = [RAW_DATA_DIR / 'future_hq', RAW_DATA_DIR / 'future_option']
SPREAD_DIR = RAW_DATA_DIR / 'spread'
INVENTORY_DIR = RAW_DATA_DIR / 'inventory'
RECEIPT_DIR = RAW_DATA_DIR / 'receipt'

CODE2NAME_PATH = SRC_DATA_FUTURE / 'code2name.csv'
HQ_COLUMNS_PATH = [SRC_DATA_FUTURE / 'columns_future.csv', SRC_DATA_FUTURE / 'columns_option.csv']

# 商品中文名和字母缩写对照表
CODE2NAME_TABLE = pd.read_csv(CODE2NAME_PATH, encoding='gb2312', header=0,
                              usecols=['code', 'market', 'exchange']).dropna()
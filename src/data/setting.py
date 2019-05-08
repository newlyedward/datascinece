# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime, timedelta

from src.api.cons import INSTRUMENT_TYPE

# 数据文件存储目录
HOME_DIR = Path(__file__).parent.parent.parent
RAW_DATA_DIR = HOME_DIR / 'data/raw'
PROCESSED_DATA_DIR = HOME_DIR / 'data/processed'
REPORTS_DIR = HOME_DIR / 'reports'
RAW_HQ_DIR = [RAW_DATA_DIR / 'hq/{}'.format(x) for x in INSTRUMENT_TYPE]

# 数据文件备份目录
BACKUP_DIR = "E:\\"

# 各市场交易起始时间 0 期货 1 期权
TRADE_BEGIN_DATE = {'cffex': [datetime(2010, 4, 30), datetime.today() + timedelta(1)],
                    'czce': [datetime(2005, 4, 29), datetime(2017, 4, 19)],
                    'shfe': [datetime(2002, 1, 8), datetime(2018, 9, 21)],
                    'dce': [datetime(2000, 5, 8), datetime(2017, 3, 31)]}

SRC_DATA_FUTURE = HOME_DIR / 'src/data/future'

DATE_PATTERN = '\d{4}[-/\._]\d{1,2}[-/\._]\d{1,2}|\d{8}'

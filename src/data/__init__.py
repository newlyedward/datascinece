# -*- coding: utf-8 -*-
import abc
from src.setting import DATA_COLLECTOR, COLLECTOR_PWD
from src.util import connect_mongo

conn = connect_mongo(db='quote', username=DATA_COLLECTOR, password=COLLECTOR_PWD)

from src.data.setting import DATE_PATTERN, INSTRUMENT_TYPE, RAW_HQ_DIR, BACKUP_DIR
from src.data.tdx import get_future_hq





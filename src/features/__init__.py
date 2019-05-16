# -*- coding: utf-8 -*-
from src.setting import DATA_ANALYST, ANALYST_PWD
from src.util import connect_mongo

conn = connect_mongo(db='quote', username=DATA_ANALYST, password=ANALYST_PWD)

from src.features.block.block import build_all_blocks

# -*- coding: utf-8 -*-
from pathlib import Path

home_dir = Path(__file__).parent.parent.parent.parent
raw_data_dir = home_dir / 'data/raw'
reports_dir = home_dir / 'reports'

CBS_URL = 'https://www.jisilu.cn/data/cbnew/cb_list/'
CBS_STOCK_DETAIL_URL = 'https://www.jisilu.cn/data/stock/'

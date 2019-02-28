# -*- coding: utf-8 -*-
tdx_dir = "D:\\Trade\\TDX\\"

MARKETS = ('cffex',  # 中金所
           'czce',  # 郑州商品交易所
           'dce',  # 大连商品交易所
           'shfe',  # 上海期货交易所
           'sse',  # 上海证券交易所
           'szse',  # 深圳证券交易所
           )

MARKET2TDX_CODE = {MARKETS[0]: '47',  # 47：中金所
                   MARKETS[1]: '28',  # 28：郑州商品交易所
                   MARKETS[2]: '29',  # 29：大连商品交易所
                   MARKETS[3]: '30',  # 30：上海期货交易所
                   }

MARKET_DIR = {'cffex': 'ds',  # 中金所
              'czce': 'ds',  # 郑州商品交易所
              'dce': 'ds',  # 大连商品交易所
              'shfe': 'ds',  # 上海期货交易所
              'sse': 'sh',  # 上海证券交易所
              'szse': 'sz',  # 深圳证券交易所
              }

PERIOD_DIR = {'d': 'lday',  # 日线
              '5m': 'fzline',  # 5分钟
              '1m': 'minline'
              }

PERIOD_EXT = {'d': '.day',  # 日线
              '5m': '.lc5',  # 5分钟
              '1m': '.lc1'
              }

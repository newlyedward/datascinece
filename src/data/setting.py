# -*- coding: utf-8 -*-
from pathlib import Path

HOME_DIR = Path(__file__).parent.parent.parent
RAW_DATA_DIR = HOME_DIR / 'data/raw'
PROCESSED_DATA_DIR = HOME_DIR / 'data/processed'
REPORTS_DIR = HOME_DIR / 'reports'

SRC_DATA_FUTURE = HOME_DIR / 'src/data/future'

DATE_PATTERN = '\d{4}[-/\._]\d{1,2}[-/\._]\d{1,2}|\d{8}'

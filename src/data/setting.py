# -*- coding: utf-8 -*-
from pathlib import Path

home_dir = Path(__file__).parent.parent.parent
raw_data_dir = home_dir / 'data/raw'
processed_data_dir = home_dir / 'data/processed'
reports_dir = home_dir / 'reports'
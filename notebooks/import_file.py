# -*- coding: utf-8 -*-
# Standard Scientific Import
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import talib

import cufflinks as cf
import plotly.offline as py
import plotly.graph_objs as go
import plotly.figure_factory as ff

cf.go_offline()  # required to use plotly offline (no account required).
py.init_notebook_mode()  # graphs charts inline (IPython).

# Module IMports
from notebooks.jupyter_lib.lib_loader import *
from IPython.core.interactiveshell import InteractiveShell

InteractiveShell.ast_node_interactivity = "all"

# Custom Import
# from pathlib import Path
# homedir = Path(Path.cwd()).parent
# RAW_DATA_DIR = homedir / 'data/raw'
# REPORTS_DIR = homedir / 'reports'
#
# import sys
# sys.path.append(str(homedir))

from src.features import *
from src.data.future import *
from src.data.future import get_spreads




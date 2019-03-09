# Standard Scientific Import
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

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
# raw_data_dir = homedir / 'data/raw'
# reports_dir = homedir / 'reports'
#
# import sys
# sys.path.append(str(homedir))

from src.data.tdx import get_future_hq
from src.features.block import TsBlock


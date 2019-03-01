# coding: utf-8

# In[1]:


from pathlib import Path

# In[2]:


homedir = Path(Path.cwd()).parent
raw_data_dir = homedir / 'data/raw'
reports_dir = homedir / 'reports'

# In[3]:


import sys

sys.path.append(str(homedir))

# In[4]:


import cufflinks as cf
import plotly.offline as py
import plotly.graph_objs as go

cf.go_offline()  # required to use plotly offline (no account required).
py.init_notebook_mode()  # graphs charts inline (IPython).

# In[5]:


from datetime import datetime, timedelta
import pandas as pd
from src.data.tdx import get_future_hq
from src.features.block import TsBlock

# In[6]:


today = datetime.today()
start = today - timedelta(250)

# ### 豆粕

# In[7]:


code = 'ML8'

# ### 日k线图

# In[8]:


hq = get_future_hq(code, start=start)

kline = go.Candlestick(x=hq.index,
                       open=hq['open'],
                       high=hq['high'],
                       low=hq['low'],
                       close=hq['close'])

# ### 获取中枢状态

# In[9]:


blocks = TsBlock(code)

# In[10]:


block_w = blocks.get_current_status(start=start, freq='w')
block_d = blocks.get_current_status(start=start)
segment_w = blocks.get_segments(start=start, freq='w')
segment_d = blocks.get_segments(start=start)

# ### 周线线段

# In[11]:


high, low = segment_w
high.rename("extreme", inplace=True)
low.rename("extreme", inplace=True)
segment_df = high.append(low)
segment_df.sort_index(inplace=True)

# In[12]:


segment_line_w = go.Scatter(x=segment_df.index,
                            y=segment_df)

# ### 日线线段

# In[13]:


high, low = segment_d
high.rename("extreme", inplace=True)
low.rename("extreme", inplace=True)
segment_df = high.append(low)
segment_df.sort_index(inplace=True)

# In[14]:


segment_line_d = go.Scatter(x=segment_df.index,
                            y=segment_df)

# In[15]:


data = [kline, segment_line_w, segment_line_d]
py.iplot(data, filename='simple_candlestick')

# In[16]:


block_w

# In[17]:


block_d

# ### 分钟K线图

# In[22]:


start = today - timedelta(7)

# In[24]:


hq = get_future_hq(code, start=start, freq='5m')

kline = go.Candlestick(x=hq.index,
                       open=hq['open'],
                       high=hq['high'],
                       low=hq['low'],
                       close=hq['close'])

# In[25]:


data = [kline]
py.iplot(data, filename='simple_candlestick')

# In[22]:


start = today - timedelta(7)

# In[19]:


block_w = blocks.get_current_status(start=start, freq='30m')
block_d = blocks.get_current_status(start=start)
segment_w = blocks.get_segments(start=start, freq='30m')
segment_d = blocks.get_segments(start=start)

# In[20]:


block_w

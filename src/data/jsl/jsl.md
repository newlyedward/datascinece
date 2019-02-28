### 可转债详细信息
***
#### 网络地址
https://www.jisilu.cn/data/cbnew/detail_hist/110044（转债代码）
#### 字段说明

##### 历史指标
|字段名|字段|示例|
|:----:|:----:|:----:|
|转债代码|bond_id|110044|
|日期|last_chg_dt|2018-11-14|
|收益率|ytm_rt|3.69%|
|转债溢价率|premium_rt|14.53%|
60天的历史指标 
### 股票详细信息
*** 
#### 网络地址
https://www.jisilu.cn/data/stock/600831-网络爬取正则提取信息
#### 字段说明
##### 详细信息表
|字段|字段(自定义)|描述|
|:---:|:---:|:----:|
|现价|sprice |
|涨幅|increase_rt |
|市值|market_value |
|有息负债率| debt_with_interest|有息负债率= 带息负债／全部投入资本，其中全部投入资本=权益+有息负债，取不到有息负债率，则用资产负债率代替|
|5年平均股息率|average_dividend_yield_5_years  |
|IPO日期| listed_dt     |
|总股份| total_shares       |
|5年平均ROE| average_roe_5_years  |
|5年营收增长率| sales_rt_5_years  |
|5年利润增长率| profit_rt_5_years |5年利润复合增长率|
|净利同比增长 | net_profit_rt_5_years |最新报告期归母净利同比增长 |
##### PE/PB温度计
|字段|描述|       
|:---:|:----:|
|当前值| |
|最大值| |
|最小值| |
|平均值| |
|百分位| |
剔除PE、PB为0与负值数据
### 转债信息
*** 
#### 网络地址 
https://www.jisilu.cn/data/cbnew/cb_list/'
#### 字段说明
| 字段名 | 字段 | 示例
|:-----------:|:--------------:|:----------:|
|转债交易所代码|bond_id|127008|
|转债名称|bond_nm|特发转债|
|转债缩写|pinyin|tfzz|
|正股代码|stock_id|sz000070|
|正股名称|stock_nm|特发信息|
|债券市场|market|sz|
|转债类型|btype|C：可转债；E：可交换债|
|净资产|stock_net_value|3.39|
|总股本（万股）|stock_amt|62699.4700|
|转股价|convert_price|6.780|
|转股起始日|convert_dt|2019-05-22|
|发行日期|issue_dt|2018-11-16|
|上市日期|list_dt|2018-04-11|
|到期日|maturity_dt|2023-11-16|
|回售起始日|next_put_dt|2021-11-16|
|回售日期|put_dt|null|
|回售说明|put_notes|null|
|回售价|put_price|100.000|
| |put_inc_cpn_fl|y|
|回售触发比|put_convert_price_ratio|66.28|
| |put_count_days|30
| |put_total_days|30
| |put_real_days|0
|标准债券折算率|repo_discount_rt|0.00
| |repo_valid_from|2018-12-03|
| |repo_valid_to|2018-12-03|
|利息|cpn_desc|第一年为 0.40%、第二年为 0.60%、第三年为 1.00%、第四年为 1.50%、第五年为 2.00%|
|到期赎回价|redeem_price|108.000|
| |redeem_inc_cpn_fl|n"
|强赎触发比（%）|redeem_price_ratio|130.000|
| |redeem_count_days|15
| |redeem_total_days|30
| |redeem_real_days|0
| |redeem_dt|null
|发行规模（亿）|orig_iss_amt|4.194|
|剩余规模（亿）|curr_iss_amt|4.194|
|向下修正|adjust_tc|在本次发行的可转债存续期间，当公司 A 股股票在任意连续二十个交易日中至少有十个交易日的收盘价低于当期转股价格的 90%时，|
|强赎条款|redeem_tc|在本次发行的可转债转股期内，如果公司 A 股股票连续三十个交易日中至少有十五个交易日的收盘价不低于当期转股价格的 130%（含）|
|回售条款|put_tc|本次发行的可转债最后两个计息年度，如果公司股票在任何连续三十个交易日的收盘价低于当期转股价格的 70%时|
|债券评级|rating_cd|AA|
|担保|guarantor|无担保"|
| |active_fl|y"
| |adq_rating|null
| |force_redeem|null
|强赎价|real_force_redeem_price|null|
|转股代码|convert_cd|未到转股期/191013|
|质押代码|repo_cd|null/105834|
| |ration|0.6689"
| |ration_cd|080070"
| |apply_cd|070070"
| |online_offline_ratio|null
|合格投资者可买|qflag|N"
| |qflag2|N"
|股东配售率|ration_rt|45.920|
|机构持仓|fund_rt|buy|
|PB|pb|2.11|
| |total_shares|626994746.0"
| |sqflg|Y"
|正股价格|sprice|7.16|
|正股成交额|svolume|8522.94|
|正股涨跌|sincrease_rt|-4.66%|
|最后更新时间|last_time|15:02:03|
|转股价值convert_value|105.60|
|溢价率|premium_rt|-5.31%|
|剩余年限|year_left|4.967|
|到期税前收益|ytm_rt|2.25%|
|到期税后收益|ytm_rt_tax|1.81%|
|现价|price|100.000|
| |full_price|100.000"
|涨跌幅|increase_rt|0.00%|
|成交额|volume|0.00|
|下修成功次数|adj_scnt|0|
|下修次数|adj_cnt|0|
|自选可转债|owned|0
| |ref_yield_info|"
| |adjust_tip|"
| |left_put_year|-"
|到期时间|short_maturity_dt|23-11-16|
|强赎触发价|force_redeem_price|8.81|
|回售触发价|put_convert_price|4.75|
|转债占比|convert_amt_ratio|9.3%|
|股票交易所代码|stock_cd|000070"
|转债代码|pre_bond_id|sz127008"
|折算率效期|repo_valid|有效期：-/有效期：2018-12-03 到 2018-12-03；质押代码：105827|
|价格提示|price_tips|待上市/全价：91.600 最后更新：15:02:03|
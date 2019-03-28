# -*- coding: utf-8 -*-


def get_trading_dates(start_date, end_date, country='cn'):
    """

    :param start_date:    datetime.datetime, 开始日期
    :param end_date:      datetime.datetime, 结束日期
    :param country:       默认是中国市场('cn')，目前仅支持中国市场
    :return:    datetime.datetime list - 交易日期列表
    """
    pass


def get_previous_trading_date(date, n=1, country='cn'):
    """

    :param date:        datetime.datetime    指定日期
    :param n:           n代表往前第n个交易日。默认为1，即前一个交易日
    :param country:     默认是中国市场('cn')，目前仅支持中国市场
    :return:            datetime.datetime - 交易日期
    """
    pass


def get_next_trading_date(date, n, country='cn'):
    """

        :param date:        datetime.datetime    指定日期
        :param n:           n代表往后第n个交易日。默认为1，即前一个交易日
        :param country:     默认是中国市场('cn')，目前仅支持中国市场
        :return:            datetime.datetime - 交易日期
        """
    pass

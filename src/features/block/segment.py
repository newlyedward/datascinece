
class KFeatures:
    """
    新产生的k线，已有的两根k线，原有最后一根k线不能确定是否形成分型
    """
    def __init__(self, datetime, high, low):
        self.datetime = datetime
        self.high = high
        self.low = low
        self.trend = None
        self.fx_type = None


class Segment:
    """
    新产生的分型线，已有的两个线段极值点，原有最后一个极值点不能确定是否线段结束
    """
    pass
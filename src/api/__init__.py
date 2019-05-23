from src.setting import DATA_ANALYST, ANALYST_PWD
from src.util import connect_mongo

conn = connect_mongo(db='quote', username=DATA_ANALYST, password=ANALYST_PWD)

from src.api.common import get_price, get_segments, get_blocks, get_peak_start_date
from src.api.futures import get_roll_yield
from src.api.cons import FREQ

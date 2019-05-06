from src.analysis.snapshot import get_snapshot
from src.setting import DATA_ANALYST, ANALYST_PWD
from src.util import connect_mongo

conn = connect_mongo(db='quote', username=DATA_ANALYST, password=ANALYST_PWD)

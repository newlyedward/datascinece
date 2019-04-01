import os
from pathlib import Path
from dotenv import load_dotenv

basedir = Path(__file__).parent.parent
load_dotenv(str(basedir / '.env'))

MONGODB_URI = os.environ.get('MONGODB_URI')
MONGODB_PORT = os.environ.get('MONGODB_PORT')

DATA_COLLECTOR = os.environ.get('DATA_COLLECTOR')
COLLECTOR_PWD = os.environ.get('COLLECTOR_PWD')
DATA_ANALYST = os.environ.get('DATA_ANALYST')
ANALYST_PWD = os.environ.get('ANALYST_PWD')

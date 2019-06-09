from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.worksheet.table import TableStyleInfo

from src.data.setting import HOME_DIR

REPORT_DIR = HOME_DIR / 'reports'

TABLE_STYLE = TableStyleInfo(name="TableStyleMedium9",
                             showFirstColumn=False,
                             showLastColumn=False,
                             showRowStripes=False,
                             showColumnStripes=True)

COLOR_RULE = ColorScaleRule(
    start_type='min', start_color='32CD32',
    mid_type='percentile', mid_value=50, mid_color='FFFF00',
    end_type='max', end_color='FF6347')

PERCENT_FORMAT = '0.00%'
PERCENT0_FORMAT = '0%'
COMMA0_FORMAT = '#,##0'
DATE_FORMAT = 'mm-dd-yy'

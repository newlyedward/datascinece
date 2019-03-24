# -*- coding: utf-8 -*-
import re
from pathlib import Path
from src.data.setting import DATE_PATTERN

if __name__ == '__main__':
    file_dir = Path(r'D:\Code\test\cookiercutter\datascience\datascinece\data\raw\hq')
    for file in file_dir.rglob('*.*'):
        filename = file.name
        target = file.parent / '{}.day'.format(re.search(DATE_PATTERN, filename)[0])
        file.rename(target)
        print(target)

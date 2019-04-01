# -*- coding: utf-8 -*-
from configparser import ConfigParser
from pathlib import Path

basedir = Path(__file__).parent


# TODO ini 文件中是否能加注释
def get_config_file(file_name='config.ini'):
    file_name = str(basedir / file_name)
    fp = open(file_name, 'w')
    return fp


def get_config_handle(file_name='config.ini'):
    config = ConfigParser()
    file_name = str(basedir / file_name)
    config.read(file_name)
    return config


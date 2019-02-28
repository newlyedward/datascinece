# -*- coding: utf-8 -*-
import os
from configparser import ConfigParser

# TODO ini 文件中加注释


def get_config_file(file_name='config.ini'):
    file_name = os.path.join(os.getcwd(), file_name)
    fp = open(file_name, 'w')
    return fp


def get_config_handle(file_name='config.ini'):
    config = ConfigParser()
    file_name = os.path.join(os.getcwd(), file_name)
    config.read(file_name)
    return config

# -*- coding: utf-8 -*-
import requests
from lxml import etree
from src.log import LogHandler

log = LogHandler('util.crawler.log')

HEADERS = {'Connection': 'keep-alive',
           'Cache-Control': 'max-age=0',
           'Upgrade-Insecure-Requests': '1',
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko)',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
           'Accept-Encoding': 'gzip, deflate, br',
           'Accept-Language': 'zh-CN,zh;q=0.8',
           }


def get_html_text(url, headers=HEADERS, encoding=None):
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        if encoding is None:
            response.encoding = response.apparent_encoding
        else:
            response.encoding = encoding
        return response.text
    except:
        log.info('{} is {}'.format(url, response.status_code))
        return response.status_code


def get_html_tree(url, headers=HEADERS, encoding=None):
    """
    获取html树
    :param url:
    :param headers:
    :param encoding:
    :return:
    """
    try:
        response = requests.get(url=url, headers=headers, timeout=30)
        response.raise_for_status()
        if encoding is None:
            response.encoding = response.apparent_encoding
        else:
            response.encoding = encoding
        return etree.HTML(response.text)
    except:
        log.info('{} is {}'.format(url, response.status_code))
        return response.status_code


def get_post_text(url, data=None, headers=HEADERS, encoding=None):

    try:
        response = requests.post(url=url, data=data, headers=headers, timeout=30)
        response.raise_for_status()
        if encoding is None:
            response.encoding = response.apparent_encoding
        else:
            response.encoding = encoding
        return response.text
    except:
        log.info('{} is {}'.format(url, response.status_code))
        return response.status_code



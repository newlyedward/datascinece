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


def get_html_text(url, headers=HEADERS):
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        return response.text
    except:
        log.info('{} is {}'.format(url, response.status_code))
        return response.status_code


def get_html_tree(url, headers=HEADERS):
    """
    获取html树
    :param url:
    :param headers:
    :return:
    """
    try:
        resp = requests.get(url=url, headers=headers, timeout=30)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding
        return etree.HTML(resp.text)
    except:
        return resp.status_code

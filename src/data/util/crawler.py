# -*- coding: utf-8 -*-
import requests
from lxml import etree

HEADERS = {'Connection': 'keep-alive',
           'Cache-Control': 'max-age=0',
           'Upgrade-Insecure-Requests': '1',
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko)',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
           'Accept-Encoding': 'gzip, deflate, sdch',
           'Accept-Language': 'zh-CN,zh;q=0.8',
           }


def get_html_text(url, headers=HEADERS):
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        return response.text
    except:
        return response.status_code


def get_html_tree(url, headers=HEADERS):
    """
    获取html树
    :param url:
    :param headers:
    :return:
    """

    html = requests.get(url=url, headers=headers, timeout=30).content
    return etree.HTML(html)

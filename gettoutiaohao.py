# -*- coding: utf-8 -*-
#停用 2016年8月11日 14:50:41
# 从头条号获取文章url
import sys
import time
import random
import requests
import gc
import simplejson
from lxml import etree
from getcategory import Category
from public.config import Config
from public.mysqlpooldao import MysqlDao
from public.redispooldao import RedisDao
from public.headers import Headers
from public.proxies import Proxies

reload(sys)
sys.setdefaultencoding('utf-8')


def doIt(author, url):
    mysql_dao = MysqlDao()
    headers = Headers.get_headers()
    proxies = Proxies.get_proxies()
    try:
        html = requests.get(url, headers=headers, timeout=30, proxies=proxies).content
        selector = etree.HTML(html)
        titles = selector.xpath('//h3/a[1]/text()')
        urls = selector.xpath('//h3/a[1]/@href')
        imgs = selector.xpath('//div[@class="list_image"]/ul[1]/li[1]/a[1]/img[1]/@src')
        next_name = selector.xpath('//*[@id="pagebar"]/a[last()]/text()')
        next_url = selector.xpath('//*[@id="pagebar"]/a[last()]/@href')
        category_id = 0
        i = 0
        print(urls)
        while True:
            if i >= len(urls):
                break
            url2 = urls[i]
            img_main = imgs[i]
            created_at = time.strftime('%Y-%m-%d %H:%M:%S')
            insert_value = '"' + str(
                    category_id) + '","' + url2 + '","' + img_main + '","' + author + '",0,"' + created_at + '"'
            sql = 'insert ignore into zmt_toutiao_url (`category_id`,`url`,`img_main`,`author`,`status`,`created_at`) values (' + insert_value + ')'
            print(sql)
            mysql_dao.execute(sql)
            i = i + 1
    except Exception as e:
        print(Exception)
        print(e)
    try:
        # 翻页
        next_name = selector.xpath('//*[@id="pagebar"]/a[last()]/text()')
        if len(next_name) > 0:
            if u'下一页' in next_name[0]:
                next_url = selector.xpath('//*[@id="pagebar"]/a[last()]/@href')[0]
                doIt(author, next_url)
    except Exception as e:
        print(Exception)
        print(e)


if __name__ == '__main__':
    mysql_dao = MysqlDao()
    while True:
        sql = 'select * from zmt_toutiaohao_url WHERE `time`=0 limit 0,1'
        ret = mysql_dao.execute(sql)
        if len(ret) == 0:
            break
        res = ret[0]
        id = res[0]
        author = res[1]
        url = res[2]
        # sql = 'update zmt_toutiaohao_url set `time`=1 where `id`=' + str(id)
        # res = mysql_dao.execute(sql)
        doIt(author, url)
    mysql_dao.close()
    print('game over')

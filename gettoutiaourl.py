# -*- coding: utf-8 -*-
# 用于爬取头条文章的url

import sys
import time
import requests
import simplejson
from getcategory import Category
from public.headers import Headers
from public.proxies import Proxies
from public.redispooldao import RedisDao

reload(sys)
sys.setdefaultencoding('utf-8')


def getUrl(url, category_id, page_time=time.time()):
    redis_dao = RedisDao()
    headers = Headers.get_headers()
    proxies = Proxies.get_proxies()
    url1 = url + str(page_time)
    req_json = requests.get(url1, headers=headers, proxies=proxies).content
    req_dict = simplejson.loads(req_json)
    data = req_dict['data']
    for d in data:
        print(d)
        url2 = d['share_url']
        img_main = ''
        if d.has_key('middle_image'):
            if isinstance(d['middle_image'], dict):
                img_main = d['middle_image']['url']
            else:
                img_main = d['middle_image']
        author = d['source']
        data = {
            'category_id': category_id,
            'url': url2,
            'img_main': img_main,
            'author': author
        }
        try:
            date = time.strftime('%Y%m%d')
            redis_dao.rpush('queue:toutiao_%s' % date, simplejson.dumps(data))
        except:
            redis_dao = RedisDao()
    try:
        next_time = req_dict['next']['max_behot_time']
        # print('page turn ', url, next_time)
    except:
        return
    if page_time != next_time and next_time != 0:
        getUrl(url, category_id, next_time)
    return


if __name__ == '__main__':
    category_list = Category.category
    for category in category_list:
        getUrl(category['url'], category['category_id'])
    print('game over')

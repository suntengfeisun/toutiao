# -*- coding: utf-8 -*-

import sys
import time
import threading
import simplejson
import requests
from lxml import etree
from public.config import Config
from public.mysqlpooldao import MysqlDao
from public.redispooldao import RedisDao
from public.headers import Headers
from public.proxies import Proxies

reload(sys)
sys.setdefaultencoding('utf-8')


# http://toutiao.com/api/article/recent/?source=2&count=20&category=__all__
class Worker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        mysql_dao = MysqlDao()
        redis_dao = RedisDao()
        while True:
            print(self.getName())
            date = time.strftime('%Y%m%d')
            data_json = redis_dao.lpop('queue:toutiao_%s' % date)
            if data_json == None:
                break
            data = simplejson.loads(data_json)
            category_id = data['category_id']
            url = data['url']
            img_main = data['img_main']
            author = data['author']
            try:
                headers = Headers.get_headers()
                proxies = Proxies.get_proxies()
                html = requests.get(url, headers=headers, timeout=30, proxies=proxies).content
                selector = etree.HTML(html)
                status = selector.xpath('//*[@id="aboutus"]/div[1]/span[1]/text()')
                if len(status) > 0:
                    if u'今日头条' in status[0]:
                        category_names = selector.xpath('//div[@class="curpos"]/a[2]/text()')
                        if len(category_names) != 0:
                            category_name = category_names[0]
                            if u'图片' in category_name and u'视频' in category_name:
                                pass
                            else:
                                if category_id != 0:
                                    toutiaohao_authors = selector.xpath('//*[contains(@class,"gc_name")]/text()')
                                    toutiaohao_urls = selector.xpath('//*[contains(@class,"gc_name")]/@href')
                                    try:
                                        toutiaohao_num = 0
                                        for toutiaohao_url in toutiaohao_urls:
                                            toutiaohao_sql = 'insert ignore into zmt_toutiaohao_url (`author`,`url`) values ("' + \
                                                             toutiaohao_authors[toutiaohao_num] + '","' + \
                                                             toutiaohao_urls[
                                                                 toutiaohao_num] + '")'
                                            toutiaohao_num = toutiaohao_num + 1
                                            mysql_dao.execute(toutiaohao_sql)
                                    except Exception as e:
                                        print(Exception)
                                        print(e)

                                    title = selector.xpath('//*[@class="title"]/text()')
                                    if len(title) > 0:
                                        title_t = title[0].replace('"', '')
                                    else:
                                        title_t = ''
                                    content = selector.xpath('//*[@class="article-content"]/descendant::text()')
                                    img = selector.xpath('//img[@onerror="javascript:errorimg.call(this);"]/@src')
                                    content_str = ''
                                    img_str = ''
                                    for c in content:
                                        content_str = content_str + '{ycontent}' + c.replace('"', '')
                                    for img_i in img:
                                        img_str = img_str + '{yimg}' + img_i.replace('"', '')
                                    time_now = time.strftime('%Y-%m-%d %H:%M:%S')
                                    time_ts = selector.xpath('//*[@class="time"]/text()')
                                    if len(time_ts) > 0:
                                        time_t = time_ts[0].replace('"', '')
                                    else:
                                        time_t = ''
                                    insert_value = '"' + str(
                                            category_id) + '","' + title_t + '","' + content_str + '","' + url + '","' + img_main + '","","' + img_str + '","","' + author + '","' + time_t + '","' + time_now + '","' + time_now + '"';
                                    sql = 'insert ignore into zmt_content (`category_id`,`title`,`content`,`url`,`img_main`,`img_main_oss`,`img`,`img_oss`,`author`,`time`,`created_at`,`updated_at`) values (' + insert_value + ')'
                                    print(sql)
                                    if content_str != '':
                                        mysql_dao.execute(sql)
            except Exception as e:
                print(Exception)
                print(e)
        mysql_dao.close()


if __name__ == '__main__':
    # time_now = time.strftime('%Y-%m-%d %H:%M:%S')
    # 初始化并启动进程
    threads = []
    for i in xrange(Config.clawler_num):
        worker = Worker()
        threads.append(worker)
    for t in threads:
        t.setDaemon(True)
        t.start()
    for t in threads:
        t.join()
    print('game over')

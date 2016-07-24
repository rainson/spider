#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Created by Rainson on 2016-03-25
# Contact: gengyusheng@gmail.com
#

import re
from celery import Celery
try:
    from pyquery import PyQuery
    _has_pyquery = True
except ImportError:
    _has_pyquery = False

import urllib
import urllib2
from datetime import datetime
import time
from gevent import monkey; monkey.patch_socket()
import gevent
from requests import ConnectionError


#import operator ##对时间排序用的



app = Celery('spider_worker.chinaso_news', backend='amqp', broker=None)#MQ_BROKER_URL)
_base_url = 'http://news.chinaso.com/newssearch.htm?' #'http://news.chinaso.com/search?'

#app.conf.update(
#    CELERY_ROUTES={
#        'spider_worker.chinaso_news.collect': {'queue': 'chinaso_news'},
#    },
#)

def remove_em_tag(s):
    return s.replace('<em>', '').replace('</em>', '')


def _get_html(url):
    send_headers = {
 #       'Host': 'www.chinaso.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; rv:16.0) Gecko/20100101 Firefox/16.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Connection': 'keep-alive'
    }
    req = urllib2.Request(url, headers=send_headers)
    response = urllib2.urlopen(req)
    html = response.read()
    response.close()
    return html


def _parse_search_page(url):
    """对爬虫的搜索网页进行信息采集，主要采集主题，链接地址，时间
       输入：爬虫得到的搜索网页界面body
       输出：包含主题，链接，日期的dict
    """
    root = PyQuery(url)#_get_html(url))
    #print root
    result = root('li.reItem')
    doc_list = []
    for node in result:
        node = PyQuery(node)
        title = node('h2 a')
        url = title.attr('href')
        title = remove_em_tag(title.html())
        date_author = node('.snapshot span')   #?
        date_author = date_author.text().split(u'-')

        try :
            assert len(date_author)==4
            author_str = date_author[-1]
        except AssertionError:
            print("网站作者信息提取有误，重新检查网站的代码")
            raise AssertionError

        pub_date = datetime(
            int(date_author[0]), int(date_author[1]), int(date_author[2]),
            )
        pub_date = time.mktime(pub_date.timetuple())
        doc_list.append(
            {
                'title': title,
                'url': url,
                'pub_date': pub_date,#[Year,Month,Day]
                'author': author_str,
#                'src': CONST.SRC_NEWS,
            }
        )


    return doc_list



class make_search:

    def __init__(
                    self,
                    or_words_list,
                    word_list_and,
                    word_list_exclude,
                    page_num
                    ):
        self.or_list = or_words_list
        self.and_list = word_list_and
        self.ex_list = word_list_exclude
        self.page_num = page_num


    def _search_url(
                        self,
                        word_list_and,
                        page = None
                        ):
        """对搜索关键字进行处理变成url地址
            输入：搜索关键字，以及要去除的关键字
            输出：包含关键字的url
        """
        ####几个不同搜索关键字组合####
        def _to_and_param(word_list):
            if word_list:
                return '%s' % '+'.join(['"%s"' % x for x in word_list])
            else:return ""
        ####要排除的关键字####
        def _to_exclude_param(word_list):
            if word_list:
                return '%s' % '+-'.join(['"%s"' % x for x in word_list])
            else:return ""

        query = {'page':page}
        word_list_exclude = self.ex_list
        key_words = '{0}{1}+-{2}'.format(
                                    'q=',
                                  _to_and_param(word_list_and),
                                  _to_exclude_param(word_list_exclude)
                                 )
        url='{0}{1}&{2}'.format(_base_url,key_words,urllib.urlencode(query))
        #print page
        return url



    def get_search_result(self):
        """
        返回包含or_lists 以及 所有搜索结果 后合并的搜索结果doc_list
        """
        doc_list = list()
        ###对于同样的关键词所有页面的搜索结果
        def _search_all_pages(word_list_and):            
            for page in range(1,self.page_num+1):
                try:
                    url = self._search_url(word_list_and=word_list_and, page=page)
                    doc_list.extend(_parse_search_page(url))
                except AttributeError:
                    break
                except AssertionError:
                    break
                
        # and 的搜索结果
        if self.and_list:
            _search_all_pages(self.and_list)
            
        # or 的搜索结果
        if self.or_list:
            for or_word in self.or_list:
                or_word = list([or_word])
                _search_all_pages(or_word)

        return doc_list

        



#from pyquery.openers import HTTPError
from urllib2 import URLError
#
def get_doc_list(pars):
    """

    :param search_key_words:
    :return doc_list:
    """
    word_list_and     = ["清明"]
    word_list_exclude =  ['旅游', '小长假']
    or_words = ['端午','七夕']
    page_num = 1#pars['pgnum']
    
    
    sear = make_search(or_words,word_list_and,word_list_exclude,page_num)
    try:
        doc_list = sear.get_search_result()   
        print(len(doc_list),pars)
        return doc_list
    except ConnectionError,e:
        print 'connectinerros',e.reason, pars
    except URLError,e:
        print 'urlerros' , e.reason , pars

    
    
    
if __name__ == "__main__":
    N=10
    # for pars in range(N):
    #     get_doc_list(pars)
    for i in range(N):
        gevent.joinall([
                gevent.spawn(get_doc_list, i) for i in range(100)

                ])



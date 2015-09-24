# -*- coding: utf-8 -*-
"""
Created on Wed Sep 09 19:16:11 2015

@author: cenkai
"""

from pyspark import SparkConf, SparkContext
import sys
from operator import add
import re
import csv
import urllib
import os

reload(sys)
sys.setdefaultencoding('utf-8')

APP_NAME = "cernet"

#输出格式为月，日，时，学校，域名，种类，关键词
def cleanse(record, ipdict, parser, air):
    tmp = record.split('|')
    if len(tmp) < 12:
        return None
    host = tmp[2]
    month = tmp[0][5:7]
    day = tmp[0][8:10]
    hour = tmp[0][11:13]
    url = host + tmp[3]
    try:
        school = ipdict[tmp[7][:-2]]
    except KeyError:
        return None
    for hostdict in parser:
        k = 0
        if hostdict['regex'].search(url):
            k = 1
            t = hostdict['type']
            try:
                keyword = hostdict['dest'](url)
            except:
                keyword = 'unknown'
            break
    if not k:
        keyword = 'unknown'
        t = 'unknown'
    return '%s|%s|%s|%s|%s|%s|%s'%(month, day, hour, school, host, t, keyword)

def stat(x, prefix, features, f):
    if len(features) == 1:
        fea = f[features[0]]
        result = x.map(lambda x: (x.split('|')[fea], 1)).reduceByKey(add).collect()
    else:
        result = stat1(x, features, f).collect()
    filename = 'result/[%s]%s.csv'%(prefix, '_'.join(features))
    output(result, filename, len(features))

def output(result, filename, n):
    if len(result) == 0:
        return
    with open(filename, 'ab+') as final:
        spamwriter = csv.writer(final, dialect='excel')
        if n-1:
            col= result[0][1]
            for line in result[1:]:
                if len(line[1]) > len(col):
                    col = line[1]
            spamwriter.writerow(['']+list(col))
            for line in result:
                row = [line[0]]
                for item in col:
                    try:
                        row.append(line[1][item])
                    except KeyError:
                        row.append(0)
                spamwriter.writerow(row)
        else:
            for line in result:
                spamwriter.writerow(list(line))

def stat1(x, features, f):
    feas = []
    for feature in features:
        feas.append(f[feature])
    return x.groupBy(lambda x : x.split('|')[feas[0]]).map(lambda x : stat2(x, feas[1]))

def stat2(x, n):
    d1 = x[0]
    d2 = {}
    for item in x[1]:
        try:
            d2[item.split('|')[n]] = d2[item.split('|')[n]] + 1
        except KeyError:
            d2[item.split('|')[n]] = 1
    return (d1, d2)

def main(sc, path):
    os.system('mkdir result')
    global ipdict, parser, air, f
    f = {'month' : 0, 'day' : 1, 'hour' : 2, 'school' : 3, 'host' : 4, 'type' : 5, 'keyword' : 6}
    air = {}
    with open('airport.txt') as airports:
        for line in airports.readlines():
            air[line.split('\t')[1].strip()]=line.split('\t')[0].replace(' ','')
    ipdict = {}
    with open('ipdict.txt') as ipdicts:
        for line in ipdicts.readlines():
             ipdict[line.split('|')[0]] = line.split('|')[1].strip().decode('utf-8', 'ignore')
    cernet = sc.textFile(path)
    parser = [{'regex': '^tieba\.baidu\.com\/+[\w\W]+kw=', 'type': u'搜索', 'dest' : lambda x: urllib.unquote(x.split('kw=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')},\
    {'regex': '^m\.tieba\.com\/+[\w\W]+word=', 'type': u'搜索', 'dest' : lambda x: urllib.unquote(x.split('word=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^(www\.b|b)aidu\.com\/+[\w\W]+wd=', 'type': u'搜索', 'dest' : lambda x: urllib.unquote(x.split('wd=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^m\.baidu\.com\/+[\w\W]+s\?word=', 'type': u'搜索', 'dest' : lambda x: urllib.unquote(x.split('s?word=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^m\.baidu\.com\/+[\w\W]+query=', 'type': u'搜索', 'dest' : lambda x: urllib.unquote(x.split('query=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^s\.lvmama\.com+[\w\W]+keyword', 'type': u'旅游', 'dest' : lambda x: urllib.unquote(x.split('keyword=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')},\
    {'regex': '^(www\.l|l)+y\.com\/huochepiao\/train\/trainsearch+[\w\W]+\-', 'type': u'旅游', 'dest' : lambda x: x.split('-')[2].split('&')[0]}, 
    {'regex': '^(www\.l|l)+y\.com\/flight\/+[\w\W]+para=0', 'type': u'旅游', 'dest' : lambda x: air[x.split('*')[2].upper()].decode('utf-8', 'ignore')},\
    {'regex': '^gny\.ly\.com\/+[\w\W]+dest=', 'type': u'旅游', 'dest' : lambda x: urllib.unquote(x.split('dest=')[1]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^trains\.ctrip\.com\/+[\w\W]+\/+[\w\W]+toCn=', 'type': u'旅游', 'dest' : lambda x: urllib.unquote(x.split('toCn=')[1].split('&')[0]).encode('raw_unicode_escape').decode('gb2312', 'ignore')}, \
    {'regex': '^flights\.ctrip\.com/booking+\/+[\w\W]+\-day', 'type': u'旅游','dest': lambda x: air[x.split('-')[1].upper()].decode('utf-8', 'ignore')}, \
    {'regex': '^flight\.qunar\.com\/site\/+[\w\W]+ArrivalAirport=', 'type': u'旅游', 'dest' : lambda x: urllib.unquote(x.split('ArrivalAirport=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^train\.qunar\.com\/+[\w\W]+toStation=', 'type': u'旅游', 'dest' : lambda x: urllib.unquote(x.split('toStation=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^travel\.qunar\.com\/search', 'type': u'旅游', 'dest' : lambda x: urllib.unquote(x.split('/')[3].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^sjipiao.alitrip.com\/flight+[\w\W]+arrCity=', 'type': u'旅游', 'dest': lambda x: air[x.split('arrCity=')[1].split('&')[0].upper()].decode('utf-8', 'ignore')}, \
    {'regex': '^train.alitrip.com\/stsSearch+[\w\W]+arrLocation=', 'type': u'旅游','dest': lambda x: urllib.unquote(x.split('arrLocation=')[1].split('&')[0]).encode('raw_unicode_escape').decode('gb2312', 'ignore')}, \
    {'regex': '^alitrip\.com\/vacation\/+[\w\W]+mq=', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('mq=')[1]).encode('raw_unicode_escape').decode('gb2312', 'ignore')}, \
    {'regex': '^s\.tuniu\.com\/+[\w\W]+\-+[\w\W]+\-', 'type': u'旅游','dest': lambda x: urllib.unquote(x.split('-')[3].strip('/')).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^flight\.elong\.com\/+[\w\W]+\-+[\w\W]+\/\?departdate=', 'type': u'旅游', 'dest': lambda x: air[x.split('-')[1].split('/')[0].upper()].decode('utf-8', 'ignore')}, \
    {'regex': '^(www\.m|m)afengwo\.cn+[\w\W]+q=', 'type': u'旅游', 'dest' : lambda x : urllib.unquote(x.split('q=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^search\.uzai\.com\/sh\/SearchResult\?keyword=', 'type': u'旅游', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('keyword=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^m\.tuniu\.com\/[\w\W]+keyword=', 'type': u'旅游', 'dest' : lambda x: urllib.unquote(x.split('keyword=')[1]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^m.ly.com\/guoneiyou\/+[\w\W]+dest=', 'type': u'旅游', 'dest' : lambda x: urllib.unquote(x.split('dest=')[1]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^m.ly.com\/flightnew\/+[\w\W]+\_', 'type': u'旅游', 'dest' : lambda x: air[x.split('_')[1].split('.html')[0].upper()].decode('utf-8', 'ignore')}, \
    {'regex': '^m.ly.com\/pub\/train\/trainsearch\-', 'type': u'旅游', 'dest' : lambda x: x.split('-')[2].split('&')[0]}, \
    {'regex': '^h5.m.taobao.com\/trip\/+[\w\W]+keywords=', 'type': u'旅游', 'dest' : lambda x: urllib.unquote(x.split('keywords=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^h5.m.taobao.com\/trip\/flight+[\w\W]+arrCityName=', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('arrCityName=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^h5.m.taobao.com\/trip\/train+[\w\W]+arrCity=', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('arrCity=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^m\.elong\.com\/+[\w\W]+endstation=', 'type': u'旅游',  'dest': lambda x: urllib.unquote(x.split('endstation=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^m\.elong\.com\/+[\w\W]+arrivalCityCode=', 'type': u'旅游', 'dest' : lambda x: air[x.split('arrivalCityCode=')[1].split('&')[0].upper()].decode('utf-8', 'ignore')}, \
    {'regex': '^touch\.qunar\.com\/+[\w\W]+destCity=', 'type': u'旅游', 'dest' : lambda x: urllib.unquote(x.split('destCity=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^touch\.qunar\.com\/+[\w\W]+endStation=', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('endStation=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^m\.lvmama\.com\/+destroute', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('destroute/')[1].split('/')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')},\
    {'regex': '^m\.lvmama\.com\/+ticket', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('ticket/')[1].split('/')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')},\
    {'regex': '^m\.ctrip\.com\/html5\/flight\/+[\w\W]+\-+[\w\W]+\.html\?Ddate', 'type': u'旅游', 'dest' : lambda x: air[x.split('-')[1].split('-')[0].upper()].decode('utf-8', 'ignore')}, \
    {'regex': '^m\.uzai\.com\/wd\/\?word=', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('word=')[1]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^qde\.qunar\.com\/html.ng\/+[\w\W]+flight+[\w\W]+to=', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('to=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^qde\.qunar\.com\/js.ng\/+[\w\W]+flight+[\w\W]+s1=', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('s1=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^qde\.qunar\.com\/+[\w\W]+hcp+[\w\W]+to=', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('to=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^dynamic\.m\.tuniu\.com+[\w\W]+keyword+[\w\W]+catId', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('keyword')[1][9:].split('catId')[0][:-9]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '%e6%90%9c%e7%b4%a2%e7%bb%93%e6%9e%9c%e9%a1%b5+[\w\W]+Lvmama', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('%e6%90%9c%e7%b4%a2%e7%bb%93%e6%9e%9c%e9%a1%b5_')[1].split('_')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^mapi\.mafengwo\.cn+[\w\W]+keyword=', 'type': u'旅游', 'dest': lambda x: urllib.unquote(x.split('keyword=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^search\.jd\.com\/Search\?keyword=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(x.split('keyword=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^search\.gome\.com\.cn+[\w\W]+question=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(x.split('question=')[1].split('&')[0]).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^search\.yhd\.com+[\w\W]+\/k+[\w\W]+\/', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('/k')[1].split('/')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^search\.suning\.com\/+[\w\W]+\/cityId=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('.com/')[1].split('/cityId=')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^search\.suning\.com\/+emall+[\w\W]+keyword=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('keyword=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^www\.amazon\.cn\/+[\w\W]+keywords=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('keywords=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^search\.dangdang\.com\/\?key=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('key=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^www\.okbuy\.com\/search\?top_key=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('top_key=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^search\.jumei\.com\/+[\w\W]+search=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('search=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^s\.vancl\.com\/search\?+k=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('k=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^www\.sfbest\.com\/+[\w\W]+search\?+[\w\W]+keyword=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('keyword=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^search\.yohobuy\.com\/\?+keyword=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('keyword=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^s\.tiantian\.com\/+[\w\W]+\?+keyword=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('keyword=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^www\.yougou\.com\/+[\w\W]+\?+keyword=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('keyword=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^www\.letao\.com\/s~+[\w\W]+\?ts', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('s~')[1].split('?ts')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    {'regex': '^search\.lefeng\.com\/+[\w\W]+search\?key=', 'type': u'购物', 'dest' : lambda x: urllib.unquote(urllib.unquote(x.split('?key=')[1].split('&')[0])).encode('raw_unicode_escape').decode('utf-8', 'ignore')}, \
    ]

    for i in xrange(len(parser)):
        parser[i]['regex'] = re.compile(parser[i]['regex'])
    cernet2 = cernet.map(lambda x : cleanse(x, ipdict, parser, air)).filter(lambda x : x!=None).cache()
    cernet_trip = cernet2.filter(lambda x: x.split('|')[5]==u'旅游').cache()
    cernet_shop = cernet2.filter(lambda x: x.split('|')[5]==u'购物').cache()
    cernet_search = cernet2.filter(lambda x: x.split('|')[5]==u'搜索').cache()
    for dim in f:
        stat(cernet2, 'total', [dim], f)
    stat(cernet2, 'total', ['month', 'hour'], f)
    stat(cernet2, 'total', ['day', 'hour'], f)
    stat(cernet_trip, 'trip',  ['keyword', 'month'], f)
    stat(cernet_trip, 'trip',  ['keyword', 'school'], f)
    stat(cernet_shop, 'shop', ['keyword', 'hour'], f)
    stat(cernet_shop, 'shop',  ['keyword', 'month'], f)
    stat(cernet_shop, 'shop', ['host', 'hour'], f)
    stat(cernet_shop, 'shop', ['host', 'month'], f)
    stat(cernet_shop, 'shop', ['host', 'day'], f)
    stat(cernet_shop, 'shop', ['host', 'school'], f)
    for dim in f:
        if dim!='type':
            stat(cernet_shop, 'shop', [dim], f)
            stat(cernet_trip, 'trip', [dim], f)
            stat(cernet_search, 'search', [dim], f)

if __name__ == "__main__":
    path = sys.argv[1]
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)
    main(sc, path)
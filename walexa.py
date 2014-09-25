#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys,os
import urllib2
from bs4 import BeautifulSoup
from re import sub
# from pymongo import MongoClient
import time
import sqlite3

import pdb

TABLE_NAME = 'stats'
DB_NAME = os.path.dirname(os.path.realpath(__file__)) + '/walexa.db'

def db_init():
    conn = sqlite3.connect(DB_NAME)
    createTableIfNotExists(conn)
    return conn

def do_save(params):
    conn = db_init()
    curr = conn.cursor()
    sql = 'INSERT INTO %s (url, country_rank, global_rank, bouncerate_val, bouncerate_info, pageview_val, pageview_info, dailytime_val, dailytime_info, datetime) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, datetime())' % TABLE_NAME
    save = curr.execute(sql, tuple(params))
    conn.commit()
    conn.close()
    return save

def do_show(url):
    sql = 'select datetime, global_rank, country_rank from {} where url like "%{}%"'.format(TABLE_NAME, url);
    conn = db_init()
    curr = conn.cursor();
    print('Data for {}'.format(url))
    for row in curr.execute(sql):
        print('\ton {} global rank is: {}, local rank: {}'.format(row[0], row[1], row[2]))


def createTableIfNotExists(conn):
    sql = 'select name from sqlite_master where type="table" and name="%s"' % TABLE_NAME
    curr = conn.cursor()
    exists = curr.execute(sql)
    if len(exists.fetchall()) < 1:
        sql = '''CREATE TABLE %s (id INTEGER PRIMARY KEY NOT NULL, url TEXT, country_rank INTEGER, global_rank INTEGER, bouncerate_val TEXT, bouncerate_info TEXT, pageview_val TEXT , pageview_info TEXT, dailytime_val TEXT, dailytime_info TEXT, datetime DATE )''' % TABLE_NAME
        curr.execute(sql)
        return conn.commit()

    return True

def openLink(url):
    _url = 'http://www.alexa.com/siteinfo/'+ url
    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11')]

    try:
        resp = opener.open(_url, None, 10)
    except Exception, e:
        print e
        sys.exit()

    return resp.read()

def get_country_rank(soup):
    span = soup.find('span', {'class': 'countryRank'})
    strong = span.find('div').find('strong')
    if strong:
        return int(strong.text.strip().replace(',',''))

    return 0

def get_global_rank(soup):
    span = soup.find('span', {'class': 'globleRank'})
    strong = span.find('div').find('strong')
    if strong:
        return int(strong.text.strip().replace(',',''))

    return 0

def get_engagement(soup):
    data = []
    section = soup.find('section', {'id' :'engagement-content'})
    for span in section.findAll('span', {'class':'span4'}, recursive=False):
        strong = span.find('strong', {'class' : 'metrics-data'});
        # percent = span.find('span', {'class': 'align-vmiddle'});
        if not strong:
            continue

        info = strong.nextSibling.nextSibling['title']
        data.append([strong.text.strip(), info])

    return data[0], data[1], data[2]

def do_get(url):
    html = openLink(url)
    soup = BeautifulSoup(html)
    div = soup.findAll('div', {'class':'data'})

    countryRank = get_country_rank(soup)
    globalRank = get_global_rank(soup)

    bounceRate, pageView, dailyTime  = get_engagement(soup)

    return [url, countryRank, globalRank] + bounceRate + pageView + dailyTime


def help():
    print('{} <url>\n{} <command> <url>'.format(sys.argv[0], sys.argv[0]))

def do_print(data):
    print('''
URL: %s\n\
Country Rank: %d\n\
Global Rank:%d\n\
Bounce Rate:%s (%s)\n\
Daily pageViews: %s (%s)\n\
Daily Time on site:%s (%s)\n''' % \
            (data[0], data[1],
                data[2], data[3],
                data[4], data[5],
                data[6], data[7], data[8]))



if __name__ == '__main__':
    if len(sys.argv) == 3:
        if sys.argv[1] == 'get':
            data = do_get(sys.argv[2])
            do_save(data)
            do_print(data)
        elif sys.argv[1] == 'show':
            do_show(sys.argv[2])
        else:
            raise Exception('Unknown command ({})'.format(sys.argv[1]))
    elif len(sys.argv) == 2:
        data = do_get(sys.argv[1])
        do_save(data)
        do_print(data)
    else:
        help()


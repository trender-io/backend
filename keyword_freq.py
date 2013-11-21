import os
import sys
import time
import json
import re
import sqlite3
import RAKE.rake as rake

AREAS = ["boston", "new_york", "los_angeles", "san_francisco", "chicago"]

FREQ_QUERY = "SELECT vals.val,COUNT(*) FROM %s tab,vals WHERE " \
             "tab.city_id = (SELECT city_id FROM cities WHERE name=?) AND " \
             "tab.val_id = vals.val_id AND " \
             "tab.tstamp BETWEEN ? AND ? GROUP BY vals.val ORDER BY COUNT(*) DESC"

TWEET_QUERY = "SELECT tweet FROM tweets WHERE " \
              "city_id = (SELECT city_id FROM cities WHERE name=?) AND " \
              "tstamp BETWEEN ? AND ?"

PUNC = re.compile(r'[^a-zA-Z]$')


# runs the RAKE algorithm on a "document" of tweets. 
# the document is just the tweets from a given city over a time period
def run_rake(city, start, end):
    db = sqlite3.connect(dbfile_)
    cursor = db.cursor()

    tweets = [tweet + '.' if m else tweet for tweet,m in ((row[0], PUNC.match(row[0])) for row in cursor.execute(TWEET_QUERY, (city, start, end)))]
    db.close()

    return rake.extractKeywords(" ".join(tweets), stopPath='RAKE/TwitterStoplist.txt')
    

dbfile_ = os.path.join('collected', 'twitter.db')

if not os.path.exists(dbfile_):
    print "MISSING TWITTER DB!"
    sys.exit()
    
db = sqlite3.connect(dbfile_)
cursor = db.cursor()

kw = {}
freq = {}
now = int(time.time())

# for each city, get the word frequencies and RAKE keywords over a few time periods
for city in AREAS:
    freq[city] = {}
    kw[city] = {}
    
    for t in ['word', 'hash']:
        freq[city][t] = {"1_hour": [row for row in cursor.execute(FREQ_QUERY % t, (city, now - 3600, now))],
                         "6_hour": [row for row in cursor.execute(FREQ_QUERY % t, (city, now - 21600, now))],
                         "12_hour": [row for row in cursor.execute(FREQ_QUERY % t, (city, now - 43200, now))],
                         "1_day": [row for row in cursor.execute(FREQ_QUERY % t, (city, now - 86400, now))],
                         "1_week": [row for row in cursor.execute(FREQ_QUERY % t, (city, now - 604800, now))],
                         "all": [row for row in cursor.execute(FREQ_QUERY % t, (city, 0, now))]}
                         
    kw[city] = {"1_hour": run_rake(city, now - 3600, now)[:20],
                "6_hour": run_rake(city, now - 21600, now)[:20],
                "12_hour": run_rake(city, now - 43200, now)[:20],
                "1_day": run_rake(city, now - 86400, now)[:20]}
db.close()

with open('freq.json', 'w') as fp:
    json.dump(freq, fp)
    fp.close()

with open('keywords.json', 'w') as fp:
    json.dump(kw, fp)
    fp.close()

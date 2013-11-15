import os
import sys
import time
import json
import sqlite3

AREAS = ["boston", "new_york", "los_angeles", "san_francisco", "chicago"]

FREQ_QUERY = "SELECT val,COUNT(*) FROM %s " \
             "WHERE city_id=(SELECT city_id FROM cities WHERE name=?) AND " \
             "tstamp BETWEEN ? AND ? GROUP BY val ORDER BY COUNT(*) DESC"

dbfile_ = 'twitter.db'


if not os.path.exists(dbfile_):
    print "MISSING TWITTER DB!"
    sys.exit()
    
db = sqlite3.connect(dbfile_)
cursor = db.cursor()

freq = {}
now = int(time.time())


# for each city, get the word frequencies over a few time periods
for city in AREAS:
    freq[city] = {}
    
    for t in ['word', 'hash']:
        freq[city][t] = {"1_hour": [row for row in cursor.execute(FREQ_QUERY % t, (city, now - 3600, now))],
                         "6_hour": [row for row in cursor.execute(FREQ_QUERY % t, (city, now - 21600, now))],
                         "12_hour": [row for row in cursor.execute(FREQ_QUERY % t, (city, now - 43200, now))],
                         "1_day": [row for row in cursor.execute(FREQ_QUERY % t, (city, now - 86400, now))],
                         "1_week": [row for row in cursor.execute(FREQ_QUERY % t, (city, now - 604800, now))],
                         "all": [row for row in cursor.execute(FREQ_QUERY % t, (city, 0, now))]}
                         
print json.dumps(freq, indent=4)

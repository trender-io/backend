import json
import time
import re
import os
import sqlite3
import threading
from Queue import Queue
from guess_language import guess_language
from shapely.geometry import Polygon, mapping, shape, asShape
from shapely import speedups
from twython import Twython, TwythonStreamer

if speedups.available:
    speedups.enable()

APP_KEY = 'JckhuMYvTreTPO4zC5fXxA'
APP_SECRET = 'QSAozi8gvr6urv7UwlHeCbAn83cwQ0TuxtfmbFTgV4'
OAUTH_TOKEN = '198988714-Xb5Ze2toM9UDHtLtQkWSvkVmHKMcAhlU5kJnkG3Y'
OAUTH_SECRET = 'WY4cg2u4Oc8LRjfZmKYs7WDbNdHXVSSTNZjZdvdyn2ROy'

WORD_REGEX = re.compile(r'^([a-zA-Z\'\"\?\.\!,]+)$')
HASH_REGEX = re.compile(r'^(\w+)$')

AREAS = {"los_angeles": Polygon([(-118.66333,33.610045), (-118.66333,34.415973), (-117.702026,34.415973), (-117.702026,33.610045)]),
         "san_francisco": Polygon([(-122.75,36.8), (-122.75,37.8), (-121.75,37.8), (-121.75,36.8)]),
         "chicago": Polygon([(-88.253174,41.520917), (-88.253174,42.122673), (-87.264404,42.122673), (-87.264404,41.520917)]),
         "boston": Polygon([(-71.315002,42.234618), (-71.315002,42.429539), (-70.927734,42.429539), (-70.927734,42.234618)]),
         "new_york": Polygon([(-74.057465,40.579542), (-74.057465,40.860564), (-73.766327,40.860564), (-73.766327,40.579542)])}

LOCS = ",".join([",".join([str(p) for p in c.bounds]) for c in AREAS.values()])


class SimpleCollector(TwythonStreamer):    
    def on_success(self, data):
        if not running_:
            self.disconnect()
        if 'text' in data:
            queue_.put(data)
            
    def on_error(self, status_code, data):
        print "TWITTER STREAM ERROR: ", status_code
        self.disconnect()


def collect_location(locs):
    stream = None
    
    while(running_):
        try:
            stream = SimpleCollector(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_SECRET)
            stream.statuses.filter(locations=locs)
        except:
            print "CAUGHT EXCEPTION, RECONNECTING"
            if stream:
                stream.disconnect()


def parse_tweets():    
    db = sqlite3.connect(dbfile_)
    cursor = db.cursor()
    cursor.execute("PRAGMA journal_mode = WAL")
    
    if cursor.fetchone()[0] != "wal":
        print "Could not set journal_mode!"
    
    wordfiles = {}
    hashfiles = {}
    linkfiles = {}
    
    for c in AREAS.keys():
        wordfiles[c] = open(os.path.join(outdir_, 'word_' + c + '.txt'), 'a')
        hashfiles[c] = open(os.path.join(outdir_, 'hash_' + c + '.txt'), 'a')
        linkfiles[c] = open(os.path.join(outdir_, 'link_' + c + '.txt'), 'a')
    
    count = 0
    
    while(running_):
        tweet = queue_.get()
        
        # place tweet into a city
        if not tweet['place'] or not tweet['place']['bounding_box']:
            continue
            
        poly = asShape(tweet['place']['bounding_box'])
        city = None
        for c,p in AREAS.items():
            if p.intersects(poly):
                city = c
                break
        
        if not city:
            city = "unknown"
        
        # parse the tweet for unique words and figure out the language
        tweet_text = tweet['text'].encode('utf-8')
        cleaned = [m.group() for m in (WORD_REGEX.match(t) for t in tweet_text.split()) if m]
        lang = guess_language(" ".join(cleaned))
        words = [w.lower().translate(None, '.?!,\"').replace("'s",'') for w in cleaned]
        
        if words and lang == 'en':
            tstamp = int(time.time())
            
            # save to sqlite db and to a set of files because we don't completely 
            # trust the sqlite db to not be corrupted at some point
            cursor.execute("INSERT INTO tweets VALUES (?, (SELECT city_id FROM cities WHERE name=?), ?)", \
                           (tstamp, city, " ".join(cleaned)))
            
            for w in words:
                cursor.execute("INSERT OR IGNORE INTO vals(val) VALUES (?)", (w,))
                cursor.execute("INSERT INTO word VALUES " \
                               "(?, (SELECT city_id FROM cities WHERE name=?), (SELECT val_id FROM vals WHERE val=?))", 
                               (tstamp, city, w))
            
            wordfiles[city].write("%d %s\n" % (tstamp, " ".join(words)))
            
            if tweet['entities']['hashtags']:
                tags = [m.group().lower() for m in (HASH_REGEX.match(h['text']) for h in tweet['entities']['hashtags']) if m]
                
                for t in tags:
                    cursor.execute("INSERT OR IGNORE INTO vals(val) VALUES (?)", (t,))
                    cursor.execute("INSERT INTO hash VALUES " \
                                   "(?, (SELECT city_id FROM cities WHERE name=?), (SELECT val_id FROM vals WHERE val=?))", 
                                   (tstamp, city, t))
                
                hashfiles[city].write("%d %s\n" % (tstamp, " ".join(tags)))
                
            if tweet['entities']['urls']:
                urls = [u['expanded_url'] for u in tweet['entities']['urls']]
                
                for u in urls:
                    cursor.execute("INSERT OR IGNORE INTO vals(val) VALUES (?)", (u,))
                    cursor.execute("INSERT INTO link VALUES " \
                                   "(?, (SELECT city_id FROM cities WHERE name=?), (SELECT val_id FROM vals WHERE val=?))", 
                                   (tstamp, city, u))
                
                linkfiles[city].write("%d %s\n" % (tstamp, " ".join(urls)))
            
            db.commit()
            
            count += 1 
            if count % 100 == 0:                
                for file in wordfiles.values() + hashfiles.values() + linkfiles.values():
                    file.flush()
    
    # done collecting, clean up
    db.commit()
    db.close()
    
    for file in wordfiles.values() + hashfiles.values() + linkfiles.values():
        file.close()


def make_db():
    db = sqlite3.connect(dbfile_)
    cursor = db.cursor()

    cursor.execute("CREATE TABLE vals (val_id INTEGER PRIMARY KEY ASC, val TEXT NOT NULL UNIQUE)")
    cursor.execute("CREATE TABLE cities (city_id INTEGER PRIMARY KEY ASC, name TEXT NOT NULL UNIQUE)")
    cursor.execute("CREATE TABLE tweets (tstamp INT, city_id INTEGER, tweet TEXT)")
    cursor.execute("CREATE TABLE word (tstamp INT, city_id INTEGER, val_id INTEGER)")
    cursor.execute("CREATE TABLE hash (tstamp INT, city_id INTEGER, val_id INTEGER)")
    cursor.execute("CREATE TABLE link (tstamp INT, city_id INTEGER, val_id INTEGER)")
    
    for city in AREAS + ["unknown"]:
        cursor.execute("INSERT INTO cities(name) VALUES (?)", (city,))
 
    db.close()


# horrible global variables. some of these could be params
queue_ = Queue()
running_ = True
outdir_ = 'collected'
dbfile_ = os.path.join(outdir_, 'twitter.db')

# setup output
if not os.path.exists(outdir_):
    os.makedirs(outdir_)

if not os.path.exists(dbfile_):
    make_db()

# start a thread for collection and parsing
threads = [threading.Thread(target=parse_tweets),
           threading.Thread(target=collect_location, args=(LOCS,))]

for t in threads:
    t.daemon = True
    t.start()

try:
    while(running_):
        for t in threads:
            t.join(0.5)
except:
    print "TERMINATING"
    running_ = False
    
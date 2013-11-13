import json
import time
import re
import os
import threading
from Queue import Queue
from guess_language import guess_language
from shapely.geometry import Polygon, mapping, shape, asShape
from shapely import speedups
from twython import Twython, TwythonStreamer

if speedups.available:
    speedups.enable()

APP_KEY = 'YRL2ooEoygBWmrqva4dQng'
APP_SECRET = '8cesaQxDxNykcqyra2ZXC2Yb6StBCsZ0lZXv2IJk'
OAUTH_TOKEN = '402933727-iCFAaiah9XVGZ4iJtYiCX6UAccXD4eZgzT0s8nKe'
OAUTH_SECRET = 'C54uvKvXDAyru9dQiUGXSEHHxGXAemBsHullBbAuM1Ww8'

WORD_REGEX = re.compile(r'^([a-zA-Z\'\"\?\.\!]+)$')

CITYS = {"los_angeles": Polygon([(-118.66333,33.610045), (-118.66333,34.415973), (-117.702026,34.415973), (-117.702026,33.610045)]),
         "san_francisco": Polygon([(-122.599182,37.848833), (-122.599182,37.848833), (-122.13501,37.818463), (-122.13501,37.649034)]),
         "chicago": Polygon([(-88.253174,41.520917), (-88.253174,42.122673), (-87.264404,42.122673), (-87.264404,41.520917)]),
         "boston": Polygon([(-71.315002,42.234618), (-71.315002,42.429539), (-70.927734,42.429539), (-70.927734,42.234618)]),
         "new_york": Polygon([(-74.057465,40.579542), (-74.057465,40.860564), (-73.766327,40.860564), (-73.766327,40.579542)])}

LOCS = ",".join([",".join([str(p) for p in c.bounds]) for c in CITYS.values()])


# horrible global variable
queue_ = Queue()
running_ = True
outdir_ = 'collected'


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
    stream = SimpleCollector(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_SECRET)
    stream.statuses.filter(locations=locs)


def parse_tweets():
    wordfiles = {}
    hashfiles = {}
    linkfiles = {}

    if not os.path.exists(outdir_):
        os.makedirs(outdir_)
    
    for c in CITYS.keys():
        wordfiles[c] = open(os.path.join(outdir_, 'word_' + c + '.txt'), 'w')
        hashfiles[c] = open(os.path.join(outdir_, 'hash_' + c + '.txt'), 'w')
        linkfiles[c] = open(os.path.join(outdir_, 'link_' + c + '.txt'), 'w')
    
    count = 0
    
    while(running_):
        tweet = queue_.get()
        
        # place tweet into a city
        poly = asShape(tweet['place']['bounding_box'])
        city = None
        for c,p in CITYS.items():
            if p.intersects(poly):
                city = c
                break
        
        if not city:
            continue
        
        # parse the tweet for unique words and figure out the language
        tweet_text = tweet['text'].encode('utf-8')
        words = [m.group().lower() for m in (WORD_REGEX.match(t) for t in tweet_text.split()) if m]
        lang = guess_language(" ".join(words))
        words = set([w.translate(None, '.?!,\'\"') for w in words])
        
        if words and lang == 'en':
            tstamp = time.time()
            wordfiles[city].write("%s %s\n" % (str(tstamp), " ".join(words)))
            
            if tweet['entities']['hashtags']:
                tags = [h['text'] for h in tweet['entities']['hashtags']]
                hashfiles[city].write("%s %s\n" % (str(tstamp), " ".join(tags)))
                
            if tweet['entities']['urls']:
                urls = [u['expanded_url'] for u in tweet['entities']['urls']]
                linkfiles[city].write("%s %s\n" % (str(tstamp), " ".join(urls)))
               
            count += 1 
            if count % 100 == 0:
                for file in wordfiles.values() + hashfiles.values() + linkfiles.values():
                    file.flush()
    
    for file in wordfiles.values() + hashfiles.values() + linkfiles.values():
        file.close()


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
    
import os
import re
import time
import json
import requests
import pandas as pd

SENTENCE_DELIM = re.compile(u'[.!?,;:\t\\-\\"\\(\\)\u2019\u2013]')

def extract_terms(tweet):
    global stoplist
    unigrams = []
    bigrams = []
    sentences = SENTENCE_DELIM.split(tweet.lower())
    
    for sentence in sentences:
        if not sentence: 
            continue
        
        # unigrams
        words = [w.replace("'s", '') for w in sentence.split()]
        unigrams.extend([w for w in words if w not in stoplist])
        
        # bigrams
        for w1,w2 in zip(words, words[1:]):
            if (not w1 in stoplist) and (not w2 in stoplist):
                bigrams.append("%s %s" % (w1, w2))
            
    return (unigrams, bigrams)


stoplistfile = 'wiki_stoplist.txt'
    
if not os.path.exists(stoplistfile):
    print "MISSING STOPLIST!"
    sys.exit()

stoplist = set()
fp = open(stoplistfile, 'r')
for l in fp.readlines():
    stoplist |= set(l.lower().rstrip().split())
fp.close()


# can only gt 500 changes per query, so shorten time period 
# to one minute and iterate over desired period.
period = 3600 * 12 # one day
now = int(time.time())
mins = range(now, now - period, -60)
ranges = zip(mins, mins[1:])
bymin = []

for start,end in ranges:
    options = {'format': 'json', 
               'action': 'query', 
               'list': 'recentchanges', 
               'rctype': 'edit',
               'rcshow': '!minor|!bot',
               'rcnamespace': 0,
               'rcprop': 'timestamp|title|ids|flags|sizes|user',
               'rclimit': 500,
               'rcstart': '%d' % start,
               'rcend': '%d' % end}

    r = requests.get("http://en.wikipedia.org/w/api.php", params=options)
    
    if r.status_code == 200:
        changes = json.loads(r.text)
    else:
        print "Error invoking api: %d" % r.status_code
        break
    
    bymin.append(pd.DataFrame(changes['query']['recentchanges']))
    
recent = pd.concat(bymin)

# filter out edits that are not substantial
#recent['pctchange'] = np.abs((recent.newlen - recent.oldlen) * 100. / recent.oldlen)
#recent = recent[lastday.pctchange >= 1.]

nchanges = pd.DataFrame(recent.groupby('pageid').size(), columns= ['nchanges'])
nchanges['title'] = [t[0] for t in recent.groupby('pageid')['title'].unique()]

# look for contentious articles
nchanges['neditors'] = recent.groupby('pageid').user.nunique()
nchanges = nchanges[nchanges.neditors >= 5]
nchanges = nchanges.sort('nchanges', ascending=False)[:50]

keywords = set()
for t in nchanges.title:
    uni,bi = extract_terms(t)
    keywords |= set(uni + bi)
    
# try to promote bigrams by removing the unigrams that comprise them
keywords -= set([s for sublist in [k.split() for k in keywords if " " in k] for s in sublist])

print json.dumps([k for k in keywords])

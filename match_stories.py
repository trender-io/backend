import os
import json
from operator import itemgetter
import pandas as pd
import urllib
import re
from datetime import datetime, timedelta
import psycopg2


SOURCES = ["cnn", "bbc", "nbcnews", "cnbc", "guardian", "nytimes"]
JSWARN = "Please turn on JavaScript. Media requires JavaScript to play."

def extract_words(df):
    allwords=[]
    for x in df['content']:
        words = re.findall(r'\b\S+\b', x)
        allwords.append(words)
    return allwords


def number_of_pos_neg(df):
    numPos=0
    numNeg=0
    for l in df['pos']:
        numPos=numPos+len(l)
    for m in df['neg']:
        numNeg=numNeg+len(m)
    total = numPos+numNeg
    pos_bias_percentage = float(numPos)/float(total) if total > 0 else 0
    return numPos, numNeg, total, pos_bias_percentage
    

def news_content(dfall, newssource):
    newssource_stories=[]
    newssource_links=[]
    for ns in dfall['link'].keys():
        if newssource in dfall['link'][ns]:
            newssource_stories.append(dfall['content'][ns])
            newssource_links.append(dfall['link'][ns])

    allwords=[]
    for s in range(0,len(newssource_stories)):
        words = re.findall(r'\b\S+\b', newssource_stories[s])
        allwords.append(words)
    return allwords,newssource_stories,newssource_links
    
    
def analyze_words(allwords):
    allneg=[]
    allpos=[]
    positive_words=[]
    negative_words=[]
    for article in allwords:
        for word in article:
            if word in poswords:
                positive_words.append(word)
            if word in negwords:
                negative_words.append(word)
        allpos.append([0] if not positive_words else positive_words)
        allneg.append([0] if not negative_words else negative_words)
        negative_words=[]
        positive_words=[]
    return pd.DataFrame({'pos':allpos,'neg':allneg})
    
    
def keyword_bias(keyword, site, bysrc):
    pos=0
    neg=0
    links = []
    links_bias=[]
    for o in range(len(bysrc[site]['words'])):
        if keyword in bysrc[site]['content'][o]:
            links.append(bysrc[site]['links'][o])
            pos=pos+ len(bysrc[site]['posneg']['pos'][o])
            neg=neg+ len(bysrc[site]['posneg']['neg'][o])
            links_bias.append(float(len(bysrc[site]['posneg']['pos'][o]))/float(len(bysrc[site]['posneg']['pos'][o])+len(bysrc[site]['posneg']['neg'][o])))
    bias = 0 if pos+neg==0 else float(pos)/float(pos+neg)
    return keyword, site, pd.DataFrame({'bias':links_bias,'links':links}), pos, pos+neg,bias


def all_keyword_bias(keyword, bysrc):
    all_data={}
    for site in SOURCES:
        all_data[site] = keyword_bias(keyword, site, bysrc)
    return all_data


def all_site_bias(keyword, bysrc):
    keyword_site_bias = 0
    everything = all_keyword_bias(keyword, bysrc)
    for s,e in everything.items():
        keyword_site_bias=keyword_site_bias+e[5]
    return float(keyword_site_bias)/float(len(everything))


outdir = 'collected'
storyfile = os.path.join(outdir, 'stories.csv')
posfile = 'positive.txt'
negfile = 'negative.txt'
posnegurl = 'http://www.unc.edu/~ncaren/haphazard/'

if not os.path.exists(storyfile):
    print "MISSING STORY FILE!"
    sys.exit(-1)

# load collected stories
stories = pd.read_csv(storyfile).reset_index()
stories = stories.drop_duplicates('title')
stories.picture = stories.picture.astype(object).fillna('')
stories.content = [unicode(c, 'utf8').replace(JSWARN, '').lower() for c in stories.content]

ts = []
# this is fucking ridiculous. why does our storeies DB have so many fucking date formats?
for p in stories.published:
    try:
        ts.append(datetime.strptime(p, "%Y-%m-%d %H:%M:%S"))
    except:
        try:
            ts.append(datetime.strptime(p, "%a, %d %b %Y %H:%M:%S"))
        except:
            try:
                ts.append(datetime.strptime(p[:-4], "%a, %d %b %Y %H:%M:%S"))
            except:
                ts.append(datetime.strptime(p[:-4], "%a, %d %b %Y %H:%M"))
stories['ts'] = ts

# throw out stories older than a few days - not ideal for pos/neg analysis, 
# but for now it is easiest to avoid older stories in this way, wihtout 
# restructuring the codebase to filter them out later.
stories = stories[stories.ts > datetime.utcnow() - timedelta(days=3)]

if not os.path.exists(posfile):
    urllib.urlretrieve(posnegurl + posfile, posfile)

if not os.path.exists(negfile):
    urllib.urlretrieve(posnegurl + negfile, negfile)
    
with open(posfile, 'r') as fp:
    poswords = fp.read().split('\n')

with open(negfile, 'r') as fp:
    negwords = fp.read().split('\n')

bysrc = {}
for src in SOURCES:
    words,content,links = news_content(stories, src)
    posneg = analyze_words(words)
    bysrc[src] = {"words": words,
                  "content": content,
                  "links": links,
                  "posneg": posneg}
                  
# load the generated trending keywords into separate sets and into a combined set
keywords = []
all_keywords = set()

with open(os.path.join(outdir, 'twitter_keywords.json')) as fp:
    keywords.append((set(map(itemgetter(0), json.load(fp)['tf_idf'][:50])), 1.5))

with open(os.path.join(outdir, 'wikipedia_keywords.json')) as fp:
    keywords.append((set(json.load(fp)[:50]), 1))

# with open(os.path.join(outdir, 'theme_keywords.json')) as fp:
#     keywords.append((set(json.load(fp)[:50]), 0.5))

for s,w in keywords:
    all_keywords |= s

# promote bigrams by removing unigrams that are part of bigrams (usually names)
del_unigrams = set([s for sublist in [k.split() for k in all_keywords if " " in k] for s in sublist])
all_keywords -= del_unigrams

# try to prioritize keywords based on the number of trending sources in which they appear
scores = []
for k in all_keywords:
    score = 0.
    for s,w in keywords:
        if k in s:
            score += 1 * w
    scores.append((k, score))

scores = sorted(scores, key=itemgetter(1), reverse=True)

# match keywords with stories
count = 0
new_stories = []

for k,s in scores:
    ranked = []
    for src in SOURCES:
        bias_by_site = all_keyword_bias(k, bysrc)
        overall_bias = all_site_bias(k, bysrc)
    
        bias = overall_bias * 0.2 + \
               bias_by_site[src][2]['bias'] * 0.4 + \
               keyword_bias(k, src, bysrc)[5] * 0.3 + \
               number_of_pos_neg(bysrc[src]['posneg'])[3]
        
        if len(bias) == 0:
            continue
        
        links = sorted(zip(bias_by_site[src][2]['links'], [abs(0.5-x) for x in bias.values]), key=itemgetter(1))
        ranked.append((src, links[0][0], links[0][1]))
    
    if len(ranked) == 0:
        # print "No articles for keyword:", k
        continue
    
    ranked = sorted(ranked, key=itemgetter(2))
    story = stories[stories.link == ranked[0][1]].iloc[0]
    # print k,story['title']
    
    new_stories.append((story['title'][:255], 
                        story['link'][:255], 
                        story['description'][:255], 
                        story['picture'] if len(story['picture']) < 256 else '', 
                        story['ts'], 
                        count))
    count += 1

newdf = pd.DataFrame(new_stories, columns=["title", "url", "desc", "img", "ts", "score"]).drop_duplicates('url')

# connect to DB
conn = psycopg2.connect("dbname=trender user=frontend password=tr3nderI0 host=trender.cow4slz21i2f.us-west-2.rds.amazonaws.com")
cursor = conn.cursor()

# figure out the latest story in the DB
last = 0
cursor.execute("SELECT id FROM STORIES ORDER BY id DESC LIMIT 1;")
row = cursor.fetchone()
if len(row) > 0:
    last = row[0]

now = datetime.utcnow()

for idx,row in newdf.iterrows():
    cursor.execute("INSERT INTO stories (title,url,extract,image,time,rating,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s);", 
                   (row['title'], row['url'], row['desc'], row['img'], row['ts'], row['score'], now, now))
    
conn.commit()

# remove old stories
if last > 0:
    cursor.execute("DELETE FROM stories WHERE id <= %s;" % (last,))
    conn.commit()

cursor.close()
conn.close()

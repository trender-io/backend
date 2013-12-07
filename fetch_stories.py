import feedparser
import json
import requests
from pattern import web
import pandas as pd
import time 
import os
import sys
import re

COLS = ['published', 'title', 'link', 'picture', 'height', 'width', 'description', 'content', 'source']


def get_recent(feed, date, url):
    """
    This functions parses ONLY CNN feeds.
    input: feed, max last modified date, url
    output: Dataframe with 1 row for each story.
    ['published', 'title', 'link','picture','height','width','description', 'content','source']
    
    """    
    df=pd.DataFrame(columns=COLS)
    for entry in feed.entries:
        #check for new story
        if entry.get('published_parsed') > date:
            data = requests.get(entry['link']).text
            dom = web.Element(data)
            cont=''
            pic=''
            wid=0
            hgt=0
            
            #extract image
            for p in dom.by_tag('img'):
                if hasattr(p,'src'):
                    if ("jpg" in p.attributes['src'] or "png" in p.attributes['src']) and "logo" not in p.attributes['src']:
                        if hasattr(p,'height') and hasattr(p,'width'):
                            if int(p.attributes['height'])>100:
                                pic= p.attributes['src']
                                wid= p.attributes['width']
                                hgt= p.attributes['height']
                                break

            # if not pic:
            #     continue
            
            #extract content
            try:
                if "rss.cnn" in str(url):
                    src = 'cnn'
                    desc = entry['summary_detail'].value.split('<')[0]
                    for p in dom.by_tag('p'):
                        if "<strong>" in p.content or "cnn_storypgraph" in " ".join(p.attributes.values()):
                            cont+= re.sub('<[^<]+?>', '', p.content)  #remove HTML tags and add to cont
                        
                elif "feed://feeds.bbci.co.uk" in str(url):
                    src = 'bbc'
                    desc = entry['summary_detail'].value.split('<')[0]
                    for div in dom.by_tag('div'):
                            if div.attributes.get('class') == "story-body":
                                for p in div.by_tag('p'):
                                    #remove HTML tags and add to cont
                                    cont+= re.sub('<[^<]+?>', '', p.content)
                                #ignore story is content less than 200 letters. Else write to DataFrame and return
                                if len(cont) >200:
                                    dicts=[{'published':entry['published'],'title':entry['title'],'link':entry['link'],
                                            'picture':pic,'height':hgt,
                                            'width':wid,'description':entry['summary_detail'].value.split('<')[0],'content':cont,'source':"bbc"}]
                                    df = df.append(dicts, ignore_index=True)
                        
                elif "feed://feeds.theguardian.com" in str(url):
                    src = 'guardian'
                    desc = entry['title_detail'].value
                    for div in dom.by_tag('div'):
                            if div.attributes.get('id') == "article-body-blocks":
                                for p in div.by_tag('p'):
                                    cont+= re.sub('<[^<]+?>', '', p.content)
                            
                elif "feed://www.cnbc.com" in str(url):
                    src = 'cnbc'
                    desc = entry['summary_detail'].value
                    for div in dom.by_tag('div'):
                            if div.attributes.get('id') == "article_body":
                                for p in div.by_tag('p'):
                                    cont+= re.sub('<[^<]+?>', '', p.content)
                            
                elif "feed://feeds.nbcnews.com" in str(url):
                    src = 'nbcnews'
                    desc = entry['summary_detail'].value.split('<')[0]
                    for div in dom.by_tag('div'):
                            if div.attributes.get('class') == "articleText":
                                for p in div.by_tag('p'):
                                    cont+= re.sub('<[^<]+?>', '', p.content)
                            
                elif "feed://rss.nytimes.com" in str(url):
                    src = 'nytimes'
                    desc = entry['summary_detail'].value.split('<')[0]
                    for div in dom.by_tag('div'):
                        # try:
                            if div.attributes.get('class') == "articleBody":
                                for p in div.by_tag('p'):
                                    cont+= re.sub('<[^<]+?>', '', p.content)
            except:
                continue
           
            #ignore story is content less than 200 letters. Else write to DataFrame and return
            if len(cont) > 200:
                df = df.append([{'published': entry['published'],
                                 'title': entry['title'],
                                 'link': entry['link'],
                                 'picture': pic,
                                 'height': hgt, 'width': wid,
                                 'description': desc,
                                 'content': cont,
                                 'source': src}], 
                               ignore_index=True)
            
    return df


def max_entry_date(feed):
    """
    This functions returns the date of the latest story in the feed.
    input: feed
    output: None or max date.
    """    
    
    try:
        feed.modified
        return feed.modified
    except:
        #get all published dates 
        entry_pub_dates = (e.get('published_parsed') for e in feed.entries)
        #eliminated nuls
        entry_pub_dates = tuple(e for e in entry_pub_dates if e is not None)
        #calculate max date and return
        if len(entry_pub_dates) > 0:
            return time.strftime("%a, %d %b %Y %H:%M:%S %Z", max(entry_pub_dates))
        return ''
    
               
def get_stories(url,etg,mdfd):
    """
    This function parses each url based on ETAG and Last Modified tag.
    input: url,ETAG,last modified date
    output: Story Dataframe, ETAG, and Last mdoifed date
    """   
    #Get stories from each feed
    try:
        feed = feedparser.parse(url, etag=etg if etg else None, modified=mdfd if mdfd else None)
    except:
        print "could not parse url",url
        return None, None, None
        
    #process new entries
    if feed.status==200:
        if not mdfd:
            since = time.gmtime(0)
        else:
            since = time.strptime(mdfd, "%a, %d %b %Y %H:%M:%S %Z")
            
        dfy = get_recent(feed, since, url)
        try:
            feed.etag
            return dfy, feed.etag, max_entry_date(feed)
        except:
            #print "This site does not support etag",url
            return dfy, '', max_entry_date(feed)
            
    # no new entries
    if feed.status==304:
        #print feed.debug_message
        return None, '', ''


outdir = 'collected'
rssfile = 'rss_urls.csv'
storyfile = os.path.join(outdir, 'stories.csv')

if not os.path.exists(outdir):
    os.makedirs(outdir)

if os.path.exists(storyfile):
    stories = pd.DataFrame.from_csv(storyfile).reset_index()
else:
    stories = pd.DataFrame(columns = COLS)

rss_urls = pd.DataFrame.from_csv(rssfile).reset_index()
rss_urls = rss_urls.astype(object).fillna('')

for idx in rss_urls.index:
    try:
        print "Fetching stories from: ", rss_urls.site[idx]
        dfint, etag, modified = get_stories(rss_urls.site[idx], rss_urls.etag[idx], rss_urls.modified[idx])
    except KeyboardInterrupt:
        print "Killed by user."
        sys.exit(-1)
    except:
        print "Error with feed, skipping for now."
        continue
        
    if dfint is not None and not dfint.empty:
        stories = stories.append(dfint, ignore_index=True)
        rss_urls.etag[idx] = etag
        rss_urls.modified[idx] = str(modified) if modified else ''


rss_urls.to_csv(rssfile, index=False, encoding='utf-8')
stories.to_csv(storyfile, index=False, encoding='utf-8')

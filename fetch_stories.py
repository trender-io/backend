import feedparser
import json
import requests
from pattern import web
import pandas as pd
import time 
import os


def get_cnn(feed,date,url):
    """
    This functions parses ONLY CNN feeds.
    input: feed, max last modified date, url
    output: Dataframe with 1 row for each story.
    ['published', 'title', 'link','picture','height','width','description', 'content']
    
    """    
    columns=['published', 'title', 'link','picture','height','width','description', 'content']
    df=pd.DataFrame(columns=columns)
    for entry in feed.entries:
        #check for new story
        if entry.get('published_parsed') > date:
            data = requests.get(entry['link']).text
            dom = web.Element(data)
            cont=''
            pic=None
            wid=0
            hgt=0
            #extract image
            for p in dom.by_tag('img'):
                if "jpg" in str(p.attributes['src']):
                    try:
                        if int(p.attributes['height'])>100:
                            picture= True
                            pic= p.attributes['src']
                            wid= p.attributes['width']
                            hgt= p.attributes['height']
                            break
                    except:
                        break
            #extract content
            for p in dom.by_tag('p'):
                try:
                    if "<strong>" in str(p.content) or "cnn_storypgraph" in str(p.attributes.values()):
                        cont+= p.content
                except:
                    continue
            #ignore story is content less than 200 letters. Else write to DataFrame and return
            if len(cont) >200:
                dicts=[{'published':entry['published'],'title':entry['title'],'link':entry['link'],
                        'picture':pic,'height':hgt,
                        'width':wid,'description':entry['summary_detail'].value.split('<')[0],'content':cont}]
                df = df.append(dicts, ignore_index=True)
            #break
    return df


def max_entry_date(feed):
    """
    This functions returns the date of the latest story in the feed.
    input: feed
    output: None or max date.
    """    
    #get all published dates 
    entry_pub_dates = (e.get('published_parsed') for e in feed.entries)
    #eliminated nuls
    entry_pub_dates = tuple(e for e in entry_pub_dates if e is not None)
    #calculate max date and return
    if len(entry_pub_dates) > 0:
        return max(entry_pub_dates)    
    return None


def entries_with_dates_after(feed, date,url):
    """
    This functions calls each site parsing routine.
    input: feed,last modified date, url
    output: Story Dataframe
    """    
    #print url,str(url)
    if "rss.cnn" in str(url):
        return get_cnn(feed,date,url)
       

def get_stories(url,etg,mdfd):
    """
    This function parses each url based on ETAG and Last Modified tag.
    input: url,ETAG,last modified date
    output: Story Dataframe, ETAG, and Last mdoifed date
    """   
    
    #Get stories from each feed
    try:
        feed = feedparser.parse(url,etag=etg,modified=mdfd)
    except:
        print "could not parse url",url
        return None,None,None
    #process new entries
    if feed.status==200:
        dfy=entries_with_dates_after(feed, mdfd,url)
        try:
            feed.etag
            return dfy,str(feed.etag),max_entry_date(feed)
        except:
            print "This site does not support etag",url
            return None,None,None
    # no new entries
    if feed.status==304:
        print feed.debug_message
        return None,None,None


outdir = 'collected'
rssfile = 'rss_urls.csv'
storyfile = os.path.join(outdir, 'stories.csv')

if not os.path.exists(outdir):
    os.makedirs(outdir)

if os.path.exists(storyfile):
    stories = pd.DataFrame.from_csv(storyfile).reset_index()
else:
    stories = pd.DataFrame(columns = ['published', 'title', 'link', 'picture', 'height', 'width', 'description', 'content'])

rss_urls = pd.DataFrame.from_csv(rssfile).reset_index()

for idx in rss_urls.index:
    try:
        print "Updating RSS Feed: %s" % rss_urls.site[idx]
        dfint,etag,modified = get_stories(rss_urls.site[idx].replace("'",""), rss_urls.etag[idx], rss_urls.modified[idx])
    
        if dfint is not None:
            if dfint.empty:
                print "no new stories from:", rss_urls.site[idx]
            
            if not dfint.empty:
                stories = stories.append(dfint, ignore_index=True)
                rss_urls.etag[idx] = etag
                rss_urls.modified[idx] = modified 
    except:
        print "Exception fetching feed, skipping for now."

rss_urls.to_csv(rssfile, index=False, encoding='utf-8')
stories.to_csv(storyfile, index=False, encoding='utf-8')

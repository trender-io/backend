import pandas as pd
import numpy as np
import datetime
import time
import pytz
from sklearn.feature_extraction.text import CountVectorizer
import operator
import warnings
import sys
import os
import json
from HTMLParser import HTMLParser

# Ignore depreciation warnings (living in the present with this course!)
warnings.filterwarnings("ignore", category=DeprecationWarning,
                        module="pandas", lineno=570)


'''
Some code to remove HTML tags from content
Source: http://stackoverflow.com/questions/753052/strip-html-from-strings-in-python
'''
class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def strip_tags_from_df(df):
    """
    Takes a dataframe and removes the tags from the 'content' column of each row
    Returns treated dataframe
    """
    content = []
    for c in df.content:
        content.append(strip_tags(c))
    df.content = content
    return df

def create_bag_of_words(df):
    """
    Takes a dataframe and returns a bag of words for all stories
    """
    text = df.content
    vectorizer = CountVectorizer(min_df=0)
    vectorizer.fit(text)
    x = vectorizer.transform(text)
    x = x.toarray()
    return vectorizer, x

def word_frequency(w, t):
    """
    Takes words and transform and returns an array of tuples with words and their frequency 
    """
    words_count = {}
    feature_names = w.get_feature_names()
    for n in range(len(feature_names)):
        c = 0
        for w in range(len(t)):
            c += t[w][n]
        words_count[feature_names[n]] = c
    return sorted(words_count.iteritems(), key=operator.itemgetter(1), reverse=True)

def find_word_frequencies(df):
    words, transform = create_bag_of_words(df)
    return word_frequency(words, transform)

def change_to_timedelta64(s):
	# Convert date format to comparative type (text to date)
    if len(s) > 1:
        return False
    year   = int(s[12:16])
    month  = s[8:11]
    day    = int(s[5:7])
    hour   = int(s[17:19])
    minute = int(s[20:22])
    second = int(s[23:25])
    tz     = s[26:29]
    if tz == 'EDT':
        tz = 'US/Eastern'
    zone = pytz.timezone(tz)
    month_num = month_numbers[month]
    time = datetime.datetime(year, month_num, day, hour, minute, second, tzinfo=zone)
    return np.datetime64(time)


outdir = 'collected'
storyfile = os.path.join(outdir, 'stories.csv')

if not os.path.exists(outdir):
    os.makedirs(outdir)

if not os.path.exists(storyfile):
    print "MISSING STORY FILE!"
    sys.exit(-1)


base_df = pd.read_csv(storyfile)

current = datetime.datetime.utcnow() # Current date
recent_period = current - datetime.timedelta(days=3) # Period we're considering
month_numbers = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12} # Months dict

# Remove tags and find word frequencies
base_df = strip_tags_from_df(base_df)
frequencies = find_word_frequencies(base_df)

# Make a dataframe with frequencies and calculate percentage of word usage
words_df = pd.DataFrame([n[0] for n in frequencies], columns=['word'])
words_df['freq'] = [n[1] for n in frequencies]
words_df.set_index("word", inplace=True)
words_df['pct'] = words_df.freq / float(np.sum(words_df.freq)) * 100
words_df['pct'] /= np.max(words_df['pct'])

# Make a second df where we'll keep only the most recent stories
filtered_df = base_df.copy()
filtered_df.published = [datetime.datetime.strptime(p, "%Y-%m-%d %H:%M:%S") for p in filtered_df.published]

# Filter the dataframe
filtered_df = filtered_df.sort(columns="published", ascending=False)
filtered_df = filtered_df[filtered_df.published > recent_period]

# Find new word frequencies
frequencies = find_word_frequencies(filtered_df)

# Match up word counts with those used in full_df (accounting for ones where count=0)
counts = []
words = [w[0] for w in frequencies]
for w in words_df.index:
    if w in words:
        pos = [i for i,x in enumerate(words) if x == w]
        count = frequencies[pos[0]][1]
        counts.append(count)
    else:
        counts.append(0)

# Add the column for frequency
words_df['recent_freq'] = counts

# Calculate percentages and change in percentage
words_df['recent_pct'] = words_df.recent_freq / float(np.sum(words_df.recent_freq)) * 100
words_df['recent_pct'] /= np.max(words_df['recent_pct'])
words_df['pct_change'] = words_df['recent_pct'] - words_df['pct']
words_df['pct_inc'] = words_df['recent_pct']/words_df['pct']

# This is output to use in analysis
themes_output = words_df[words_df.freq>20].sort(columns=['pct_inc', 'pct'], ascending=False)

print json.dumps([w for w in themes_output.index][:20])

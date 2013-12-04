#!/bin/bash

cd ~/data_science
python tweet_keywords.py > collected/twitter_keywords.json
cp collected/twitter_keywords.json ~/Dropbox/data_science

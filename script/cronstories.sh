#!/bin/bash

cd ~/data_science
python fetch_stories.py >> /dev/null
mv -f collected/stories.csv.new collected/stories.csv

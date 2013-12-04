#!/bin/bash

cd ~/data_science
python theme_keywords.py > collected/theme_keywords.json
cp collected/theme_keywords.json ~/Dropbox/data_science

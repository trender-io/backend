#!/bin/bash

cd ~/data_science
python wiki_keywords.py > collected/wikipedia_keywords.json.new
mv collected/wikipedia_keywords.json.new collected/wikipedia_keywords.json
cp collected/wikipedia_keywords.json ~/Dropbox/data_science

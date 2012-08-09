#!/usr/bin/env python

import sys
import nltk
tokenizer = None
tagger = None

def init_nltk():
    global tokenizer
    global tagger
    tokenizer = nltk.tokenize.RegexpTokenizer(r'\w+|[^\w\s]+')
    tagger = nltk.UnigramTagger(nltk.corpus.brown.tagged_sents())

def tag(text):
    global tokenizer
    global tagger
    if not tokenizer:
        init_nltk()
    tokenized = tokenizer.tokenize(text)
    tagged = tagger.tag(tokenized)
    return tagged
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    line = line[line.find("\t") + 1:]
    tagged = tag(line)

    for word,tags in tagged:
	print '%s\t%s' % (word, tags) 

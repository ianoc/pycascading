#!/usr/bin/env python

import sys
# input comes from STDIN (standard input)
line = sys.stdin.readline()
while len(line) > 0:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
    words = words[1:]
    # increase counters
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print '%s\t%s' % (word, 1)
    print ''
    line = sys.stdin.readline()

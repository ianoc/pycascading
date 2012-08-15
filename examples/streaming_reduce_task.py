#!/usr/bin/env python

import sys

current_word = None
current_count = 0
word = None
EOF = False
f = open("/tmp/reduce_input", "w")
# input comes from STDIN
group = sys.stdin.readline()
while len(group) > 0:
    # remove leading and trailing whitespace
    group = group.strip()
    count = 0
    tuple = sys.stdin.readline()
    if len(tuple) == 0:
        EOF = True
    else:
        while len(tuple) > 1 and EOF == False:
            tuple = tuple.strip()
            count = count + 1
            tuple = sys.stdin.readline()
            if len(tuple) == 0:
                EOF = True
    print "%s\t%d\n" % (group, count) 
    group = sys.stdin.readline()

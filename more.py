#!/usr/bin/env python
#coding:utf-8



def more(text,numlines = 20):
    lines = text.splitlines()
    while lines:
        chunk = lines[:numlines]
        lines = lines[numlines:]
        for line in chunk:print(line)
        if lines and input('More?') not in ['y','Y']:break
if __name__ ==  '__mian__':
    import sys
    more(open(sys.argv[1]).read(),10)

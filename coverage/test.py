#!/usr/bin/env python

def lls(s):
    d = {}
    start = 0
    ans = 0
    for i,c in enumerate(s):
        print "i,c %s %s" %(i,c)
        if c in d:
            print "c=%s" %c
            start = max(start, d[c] + 1)
            print "start=%s" %start
        d[c] = i
        print "d = %s" %d
        ans = max(ans, i - start + 1)
        print "ans=%s" %ans
    print d
    print ans

lls("afal")

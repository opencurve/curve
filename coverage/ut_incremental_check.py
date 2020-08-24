#!/usr/bin/python
# -*- coding: utf-8 -*-
######################################################################
# Purpose:    calculate UT coverage of git commits' new code
# Useage:    ./ut_incremental_check.py
# Version:    Initial    Version    by wahaha02
######################################################################

__version__ = 'V1.0'
__author__ = 'wahaha02'
__date__ = '2016-7-25'
__doc__ = '''
PURPOSE:
calculate UT coverage of git commits' new code

USAGE:
./ut_incremental_check.py <since>..<until> <monitor_c_files> <lcov_dir> <threshold>
example: 
./ut_incremental_check.py "227b032..79196ba" '["source/soda/sp/lssp/i2c-v2/ksource"]' "coverage" 0.6

WORK PROCESS:
get changed file list between <since> and <until> , filter by <monitor_c_files> options;
get changed lines per changed file;
based on <lcov_dir>, search .gcov.html per file, and get uncover lines;
create report file:ut_incremental_check_report.html and check <threshold> (cover lines/new lines).

UT:
./ut_incremental_check.py ut
'''

__todo__ = '''
TODO LIST:
    1. support svn
    2. refactory html report by django web template
    3. add commit info in html report
    4. prompt user/commit/date info when mouse point to uncovered line
    5. ...
'''

import sys, os, re
import json
import commands
from HTMLParser import HTMLParser
from pprint import *
 
DEBUG = 0 

class GcovHTMLParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.uncovers = []
        self.covers = []
        self.islineNum = False
        self.lineNum = 0

    def handle_starttag(self, tag, attrs):
        if tag == "span":
            for a in attrs:
                if a == ('class', 'lineNum'):
                    self.islineNum = True
                if a == ('class', 'lineNoCov'):
                    self.uncovers.append(self.lineNum)
                if a == ('class', 'lineCov'):
                    self.covers.append(self.lineNum)
                    
    def handle_data(self, data):
        if self.islineNum:
            try:
                self.lineNum = int(data)
            except:
                self.lineNum = -1
            
    def handle_endtag(self, tag):
        if tag == "span":
            self.islineNum = False
    
class UTCover(object) :
    def __init__(self, since_until, monitor, lcov_dir, thresh) :
        self.since, self.until = since_until.split('..')
        self.monitor = json.loads(monitor)
        self.lcov_dir = lcov_dir
        self.thresh = float(thresh)
        
    def get_src(self):
        # self.since, self.until, self.monitor
        satus, output = commands.getstatusoutput("git diff --name-only %s %s" %(self.since, self.until))
        src_files = [f for f in output.split('\n') 
                        for m in self.monitor if m in f 
                        if os.path.splitext(f)[1][1:] in ['c', 'cpp']]
        if DEBUG: pprint(src_files)
        for i in src_files:
            if i.find("main") > 0:
                src_files.remove(i)
        return src_files
    
    def get_change(self, src_files):
        # self.since, self.until
        changes = {}
        for f in src_files:
            satus, output = commands.getstatusoutput("git log --oneline %s..%s %s | awk '{print $1}'" %(self.since, self.until, f))
            commits = output.split('\n')
            cmd = "git blame %s | grep -E '(%s)' | awk  -F' *|)' '{print $6}'" %(f, '|'.join(commits))
            satus, lines = commands.getstatusoutput(cmd)
            changes[f] = [ int(i) for i in lines.split('\n') if i.isdigit() ]

            
        if DEBUG: pprint(changes)
        return changes
    
    def get_ghp(self, f):
        gcovfile = os.path.join(self.lcov_dir, f + '.gcov.html')
        if not os.path.exists(gcovfile):
            return None
            
        ghp = GcovHTMLParser()
        ghp.feed(open(gcovfile, 'r').read())

        return ghp
            
    def get_lcov_data(self, changes):
        # self.lcov_dir
        uncovers = {}
        lcov_changes = {}
        
        for f, lines in changes.items():
            #print f
            f = f.split('/', 1)[1]
            #f = [c.split('/', 1)[1] for c in f]
            ghp = self.get_ghp(f)
            if not ghp:
                uncovers[f] = lines
                lcov_changes[f] = lines
                continue
        
            if DEBUG: print f, ghp.uncovers, ghp.covers, lines
            lcov_changes[f] = sorted(list(set(ghp.uncovers + ghp.covers) & set(lines)))
            uncov_lines = list(set(ghp.uncovers) & set(lines))
            if len(uncov_lines) != 0:
                uncovers[f] = sorted(uncov_lines)
            ghp.close()    
        
        return lcov_changes, uncovers
    
    def create_uncover_trs(self, uncovers):
        tr_format = '''
    <tr>
      <td class="coverFile"><a href="%(file)s.gcov.html">%(file)s</a></td>
      <td class="coverFile">%(uncov_lines)s </td>
    </tr>
    
        '''
        trs = ''
        for f,v in uncovers.items():
            gcovfile = os.path.join(self.lcov_dir, f + '.gcov.html')
            if os.path.exists(gcovfile):
                s = ''
                p = re.compile(r'^<span class="lineNum">\s*(?P<num>\d+)\s*</span>')
                for line in open(gcovfile, 'r').readlines():
                    ps = p.search(line)
                    if ps:
                        s += '<a name="%s">' %ps.group('num') + line + '</a>'
                    else:
                        s += line
                open(gcovfile, 'w').write(s)    

            data = {'file':f, 'uncov_lines':
                ", ".join(['<a href="%s.gcov.html#%d">%d</a>' %(f, i, i) for i in v])}
            trs += tr_format %data
            
        return trs
        
    def create_report(self, changes, uncovers):
        change_linenum, uncov_linenum = 0, 0
        for k,v in changes.items():
            change_linenum += len(v)
        for k,v in uncovers.items():
            uncov_linenum += len(v)
        
        cov_linenum = change_linenum - uncov_linenum
        coverage = round(cov_linenum * 1.0 / change_linenum 
                            if change_linenum > 0 else 1, 4)
        
        template = open('ut_incremental_coverage_report.template', 'r').read()
        data = {    'cov_lines':cov_linenum,
                    'change_linenum':change_linenum,
                    'coverage': coverage * 100,
                    'uncover_trs': self.create_uncover_trs(uncovers)}
        open(os.path.join(self.lcov_dir, 'ut_incremental_coverage_report.html'),
            'w').write(template %data)    
        
        return coverage
    
    def check(self):
        # main function
        src_files = self.get_src()
        changes = self.get_change(src_files)
        lcov_changes, uncovers = self.get_lcov_data(changes)
        return 0 if self.create_report(lcov_changes, uncovers) > self.thresh else -1 
        
if len(sys.argv) == 1:
    print __doc__
    sys.exit(0)
if sys.argv[1] == 'ut':
    monitor, lcov_dir, threshold = ['["src/"]', "/root/.cache/bazel/_bazel_root/6f45ce5b16fddc736efcfa6b0a4c96a2/execroot/curve/bazel-out/k8-dbg/bin/coverage", 0.95]
    test1 = ["960fc84f..9a928260", monitor, lcov_dir, threshold]
    if DEBUG: print "test1: ", test1
    ut = UTCover(*test1)
    src_files = ut.get_src()
    print src_files
    print "====================="
    changes = ut.get_change(src_files)
    print "====================="
    lcov_changes, uncovers = ut.get_lcov_data(changes)
    print "====================="
    print uncovers
    print "====================="
    rate = ut.create_report(changes, uncovers)
    print rate
    print "===================="
    
    
    sys.exit(0)
    
ret = UTCover(*sys.argv[1:]).check()
sys.exit(ret)


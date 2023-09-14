#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time

import subprocess
import time
import sys

def run_exec(cmd, lock=None):
    '''
    @param log_id 
    @param cmd 
    @param lock 
    @return: None

    '''
    try:
        if lock:
            lock.acquire()
        ret_code = subprocess.call(cmd, shell=True)
    except Exception as e:
        raise e
    finally:
        if lock:
            lock.release()

    if ret_code == 0:
        return 0
    else:
        return -1

class CFioCfg:
    rw = 'randrw'
    direct = 1
    numjobs = 1
    ioengine = 'psync'
    #ioengine = 'libaio'
    bsrange = '4k-4k'
    iops = 0
    runtime = 300
    ramp_time = 5
    rwmixwrite = 30
    rwmixread = -1
    cfgPath = ''
    device = ''
#  configure your own information
#The performance test results will be saved in the execution directory perf-test

    size = '50G'
    testpath = '/home/curvefs'
    filenum = 5
    
    def writeCfg(self):
        if (self.rw == 'randrw' or self.rw == 'rw') and self.rwmixwrite > 0:
            self.cfgName = "%s_mixwrite%d_%s_%03djobs_iops%06d.cfg" % (self.rw, self.rwmixwrite, self.bsrange, self.numjobs, self.iops)
        elif (self.rw == 'randrw' or self.rw == 'rw') and self.rwmixread > 0:
            self.cfgName = "%s_mixread%d_%s_%03djobs_iops%06d.cfg" % (self.rw, self.rwmixread, self.bsrange, self.numjobs, self.iops)
        else:
            self.cfgName = "%s_%s_%03djobs_iops%06d.cfg" % (self.rw, self.bsrange, self.numjobs, self.iops)

        f = open(self.cfgPath + '/' + self.cfgName, "w")

        f.write("[global]\n")
        f.write("rw=%s\n" % self.rw)
        if (self.rw == 'randrw' or self.rw == 'rw') and self.rwmixwrite >0:
            f.write("rwmixwrite=%d\n" % self.rwmixwrite)
        elif (self.rw == 'randrw' or self.rw == 'rw') and self.rwmixread >0:
            f.write("rwmixread=%d\n" % self.rwmixread)
        f.write("direct=%d\n" % self.direct)
        f.write("size=%s\n" % self.size)
        f.write("numjobs=%d\n" % self.numjobs)
#        f.write("iodepth=%d\n" % self.numjobs)
        f.write("ioengine=%s\n" % self.ioengine)
        f.write("bsrange=%s\n" % self.bsrange)
        if self.iops > 0:
            f.write("rate_iops=%d\n" % (self.iops / self.numjobs))
        f.write("ramp_time=%d\n" % self.ramp_time)
        f.write("runtime=%d\n" % self.runtime)
        f.write("group_reporting\n")

        for i in range(0,self.filenum):
            j = i + 1
            f.write("\n[disk0%d]\n"%j)
            f.write("filename=%s/%d.txt\n"%(self.testpath,j))
        f.close()

    def getCfgName(self):
        return self.cfgName

if __name__ == "__main__":
    cfg = CFioCfg()
    cfg.runtime = 300
    cfg.ramp_time = 10
    deviceFlag = 0
    deviceList = ['']
    deviceList[0] = cfg.testpath
    testBasePath = 'perf-test'
    testCfgPath = '%s/cfg' % testBasePath
    testFioDataPath = '%s/fiodata' % testBasePath
    if not os.path.isdir(testBasePath):
        os.makedirs(testBasePath)
    if not os.path.isdir(testCfgPath):
        os.makedirs(testCfgPath)
    if not os.path.isdir(testFioDataPath):
        os.makedirs(testFioDataPath)

    # rand test
    for bsrange in ['512k-512k','128k-128k','4k-4k']:
        for numjobs in [1,4]:
            for rw in ['write','read','randwrite','randread']:
                cfg.rwmixwrite = 30
                for iops in [000]:
                    cfg.rw = rw
                    cfg.iops = iops
                    cfg.numjobs = numjobs
                    cfg.bsrange = bsrange
                    cfg.cfgPath = testCfgPath
                    # set device name
                    deviceIndex = deviceFlag % len(deviceList)
                    cfg.device = deviceList[deviceIndex]

                    cfg.writeCfg()
                    cfgFileName = cfg.getCfgName()
                    ts = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))
                    print "================ %s_%s ================" % (cfgFileName[:-4], ts)
                    cmd = " sudo fio %s/%s | tee -a %s/%s_%s.txt 2>&1" % (testCfgPath, cfgFileName, testFioDataPath, cfgFileName[:-4], ts)
                    print cmd
                    clear = "echo 3 | sudo tee -a /proc/sys/vm/drop_caches"
                    os.system(clear)
                    os.system(cmd)
                    deviceFlag = deviceFlag + 1

#!/usr/bin/env python
# -*- coding: utf8 -*-



import logging
import logging.handlers
import os
import sys

logger = None
logger2 = None
logger3 = None

class Log():
    stdout = True
    __handler = False

    def __init__(self, log_file, level=logging.DEBUG, stdout=True):
        self.__handler = logging.getLogger(log_file) 
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        log_fileHdl = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=10)
        log_fileHdl.setFormatter(formatter)
        self.__handler.addHandler(log_fileHdl)

        if stdout:
            stdoutHdl = logging.StreamHandler(sys.stdout)
            stdoutHdl.setFormatter(formatter)
            self.__handler.addHandler(stdoutHdl)
        else:
            stdoutHdl = logging.StreamHandler()
            stdoutHdl.setFormatter(formatter)
            self.__handler.addHandler(stdoutHdl)
 
        self.__handler.setLevel(level)
    
        
    def set_level(self,level):
        self.setLevel(level)
    
    def info(self,*args):
        if self.__handler:
            if len(args) == 1:
                self.__handler.info(args[0].replace('\n', '\n    '))
            elif len(args) == 2:
                self.__handler.info("logid=%s, %s" %
                              (args[0], args[1].replace('\n', '\n    ')))
            else:
                pass
    
    
    def debug(self,*args):
        if self.__handler:
            if len(args) == 1:
                self.__handler.debug(args[0].replace('\n', '\n    '))
            elif len(args) == 2:
                self.__handler.debug("logid=%s, %s" %
                               (args[0], args[1].replace('\n', '\n    ')))
            else:
                pass
    
    
    def warn(self,*args):
        if self.__handler:
            if len(args) == 1:
                self.__handler.warn(args[0].replace('\n', '\n    '))
            elif len(args) == 2:
                self.__handler.warn("logid=%s, %s" %
                              (args[0], args[1].replace('\n', '\n    ')))
            else:
                pass
    
    
    def error(self,*args):
        if self.__handler:
            if len(args) == 1:
                self.__handler.error(args[0].replace('\n', '\n    '))
            elif len(args) == 2:
                self.__handler.error("logid=%s, %s" %
                               (args[0], args[1].replace('\n', '\n    ')))
            else:
                pass
    
    
    def critical(self,*args):
        if self.__handler:
            if len(args) == 1:
                self.__handler.critical(args[0].replace('\n', '\n    '))
            elif len(args) == 2:
                self.__handler.critical("logid=%s, %s" %
                                  (args[0], args[1].replace('\n', '\n    ')))
            else:
                pass

if os.path.exists("/var/lib/jenkins/workspace/curve_failover/robot/"):
    logger = Log("/var/lib/jenkins/workspace/curve_failover/robot/curve-test.log", logging.DEBUG, False)
    logger2 = Log("/var/lib/jenkins/workspace/curve_failover/robot/curve-snapshot.log", logging.INFO, True)
    logger3 = Log("/var/lib/jenkins/workspace/curve_failover/robot/curve-attach.log", logging.DEBUG, False)
else:
    print "log dir not created!"

#!/usr/bin/env python
# -*- coding: utf8 -*-



import logging
import logging.handlers
import os


__logger = None


def init_logger(log_file, level=logging.DEBUG, stdout=True):
    '''　初使化主程序日志的函数

    @param level 打印日志的级别，默认为logging.DEBUG
    @param stdout 日志是否输出到标准输出中，默认输出
    '''

    global __logger

    __logger = logging.getLogger(log_file)

    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    if log_file is not None:
        log_fileHdl = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=10)
        log_fileHdl.setFormatter(formatter)
        __logger.addHandler(log_fileHdl)

    if stdout:
        stdoutHdl = logging.StreamHandler()
        stdoutHdl.setFormatter(formatter)
        __logger.addHandler(stdoutHdl)

    __logger.setLevel(level)


def set_level(level):
    if __logger:
        __logger.setLevel(level)


def info(*args):
    if __logger:
        if len(args) == 1:
            __logger.info(args[0].replace('\n', '\n    '))
        elif len(args) == 2:
            __logger.info("logid=%s, %s" %
                          (args[0], args[1].replace('\n', '\n    ')))
        else:
            pass


def debug(*args):
    if __logger:
        if len(args) == 1:
            __logger.debug(args[0].replace('\n', '\n    '))
        elif len(args) == 2:
            __logger.debug("logid=%s, %s" %
                           (args[0], args[1].replace('\n', '\n    ')))
        else:
            pass


def warn(*args):
    if __logger:
        if len(args) == 1:
            __logger.warn(args[0].replace('\n', '\n    '))
        elif len(args) == 2:
            __logger.warn("logid=%s, %s" %
                          (args[0], args[1].replace('\n', '\n    ')))
        else:
            pass


def error(*args):
    if __logger:
        if len(args) == 1:
            __logger.error(args[0].replace('\n', '\n    '))
        elif len(args) == 2:
            __logger.error("logid=%s, %s" %
                           (args[0], args[1].replace('\n', '\n    ')))
        else:
            pass


def critical(*args):
    if __logger:
        if len(args) == 1:
            __logger.critical(args[0].replace('\n', '\n    '))
        elif len(args) == 2:
            __logger.critical("logid=%s, %s" %
                              (args[0], args[1].replace('\n', '\n    ')))
        else:
            pass

if os.path.exists("/root/workspace/curve/curve_multijob/Resources/log"):
    init_logger("/root/workspace/curve/curve_multijob/Resources/log/curve-test.log", logging.DEBUG, True)
else:
    print "log dir not created!"

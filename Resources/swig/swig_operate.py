#!/usr/bin/env python
# -*- coding: utf8 -*-

from curvefs_python import curvefs
from config import config
from logger import logger


class LibCurve:

    def __init__(self):
        rc = curvefs.Init(config.client_conf)
        logger.info("init success.")
        if rc != 0:
            print ("init client fail! rc=%s" % rc)
            logger.error("init client fail! rc=%s" % rc)
            raise AssertionError

    def libcurve_create(self, file_path, user_name, size, pass_word=""):
        user_info_t = curvefs.UserInfo_t()
        user_info_t.owner = user_name
        user_info_t.password = pass_word
        rc = curvefs.Create(file_path, user_info_t, size)
        if rc != 0:
            print("create file fail! rc=%s" %rc)
            logger.error("create file fail! rc=%s" % rc)
            return rc
            #raise AssertionError
        else:
            return rc

    def libcurve_open(self, file_path, user_name, pass_word=""):
        user_info_t = curvefs.UserInfo_t()
        user_info_t.owner = user_name
        user_info_t.password = pass_word
        fd = curvefs.Open(file_path, user_info_t)
        logger.info("fd=%s" % fd)
        print("fd=%s" % fd)
        return fd

    def libcurve_write(self, fd, buf, offset, length):
        rc = curvefs.Write(fd, buf, offset, length)
        if rc < 0:
            print("write error, rc=%s" % rc)
            logger.error("write error, rc=%s" % rc)
            return rc
            raise AssertionError
        else:
            return rc

    def libcurve_read(self, fd, buf, offset, length):
        content = curvefs.Read(fd, buf, offset, length)
        print content
        return content

    def libcurve_statfs(self, file_name, user_name, pass_word=""):
        user_info_t = curvefs.UserInfo_t()
        user_info_t.owner = user_name
        user_info_t.password = pass_word
        file_info = curvefs.FileInfo_t()
        rc = curvefs.StatFile(file_name, user_info_t, file_info)
        if rc == 0:
            return file_info
        else:
            print "stat fs fail. rc=%s" % rc
            return rc
            raise AssertionError

    def libcurve_extend(self, file_path, user_name, new_size, pass_word=""):
        user_info_t = curvefs.UserInfo_t()
        user_info_t.owner = user_name
        user_info_t.password = pass_word
        rc = curvefs.Extend(file_path, user_info_t, new_size)
        if rc != 0:
            print "extend file fail. rc=%s" %rc
            logger.error("extend file fail. rc=%s" %rc)
            return rc
            #raise AssertionError
        else:
            return rc

    def libcurve_close(self, fd):
        rc = curvefs.Close(fd)
        if rc != 0:
            print "close file fail! rc=%s" % rc
            logger.error("close file fail! rc=%s" % rc)
            return rc
            #raise AssertionError
        else:
            return rc

    def libcurve_rename(self, user_name, old_path, new_path, pass_word=""):
        user_info_t = curvefs.UserInfo_t()
        user_info_t.owner = user_name
        user_info_t.password = pass_word
        rc = curvefs.Rename(user_info_t, old_path, new_path)
        if rc != 0:
            print "rename file fail! rc=%s" % rc
            logger.error("rename file fail! rc=%s" % rc)
            return rc
            raise AssertionError
        else:
            return rc

    def libcurve_delete(self, filepath, user_name, pass_word=""):
        user_info_t = curvefs.UserInfo_t()
        user_info_t.owner = user_name
        user_info_t.password = pass_word
        rc = curvefs.Unlink(filepath, user_info_t)
        if rc != 0:
            print "delete file fail! rc=%s" % rc
            logger.error("delete file fail! rc=%s" % rc)
            return rc
            #raise AssertionError
        else:
            return rc

    def libcurve_rmdir(self, dirpath, user_name, pass_word=""):
        user_info_t = curvefs.UserInfo_t()
        user_info_t.owner = user_name
        user_info_t.password = pass_word
        rc = curvefs.Rmdir(dirpath, user_info_t)
        if rc != 0:
            print "delete dir fail! rc=%s" % rc
            logger.error("delete dir fail! rc=%s" % rc)
            return rc
            #raise AssertionError
        else:
            return rc

    def libcurve_mkdir(self, dirpath, user_name, pass_word=""):
        user_info_t = curvefs.UserInfo_t()
        user_info_t.owner = user_name
        user_info_t.password = pass_word
        rc = curvefs.Mkdir(dirpath, user_info_t)
        if rc != 0:
            print "mkdir fail! rc=%s" % rc
            logger.error("mkdir fail! rc=%s" % rc)
            return rc
            #raise AssertionError
        else:
            return rc

def libcurve_uninit():
    rc = curvefs.UnInit()
    if rc != None:
        print "uninit  fail! rc=%s" % rc
        logger.error("uninit file fail! rc=%s" % rc)
        return rc
        raise AssertionError
    else:
        return 0






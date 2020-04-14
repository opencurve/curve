#!/usr/bin/env python
# -*- coding: utf8 -*-

from curvefs_python import curvefs
from config import config
from logger.logger import *


class LibCurve:

    def __init__(self):
        rc = curvefs.Init(config.client_conf)
        logger.info("init success.")
        if rc != 0:
            print ("init client fail! rc=%s" % rc)
            logger.debug("init client fail! rc=%s" % rc)
            raise AssertionError

    def libcurve_create(self, file_path, user_name, size, pass_word=""):
        user_info_t = curvefs.UserInfo_t()
        user_info_t.owner = user_name
        user_info_t.password = pass_word
        rc = curvefs.Create(file_path, user_info_t, size)
        if rc != 0:
#            print("create file %s fail! rc=%s" %(file_path,rc))
            logger.debug("create file %s fail! rc=%s" % (file_path,rc))
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
        return fd

    def libcurve_write(self, fd, buf, offset, length):
        rc = curvefs.Write(fd, buf, offset, length)
        if rc < 0:
            logger.debug("write error, rc=%s" % rc)
            return rc
            raise AssertionError
        else:
            return rc

    def libcurve_read(self, fd, buf, offset, length):
        content = curvefs.Read(fd, buf, offset, length)
        #logger.debug(content)
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
            logger.debug("statfs file %s fail! rc=%s" % (file_name,rc))
            return rc
            raise AssertionError

    def libcurve_extend(self, file_path, user_name, new_size, pass_word=""):
        user_info_t = curvefs.UserInfo_t()
        user_info_t.owner = user_name
        user_info_t.password = pass_word
        rc = curvefs.Extend(file_path, user_info_t, new_size)
        if rc != 0:
            logger.info("extend file fail. rc=%s" %rc)
            return rc
            #raise AssertionError
        else:
            return rc

    def libcurve_close(self, fd):
        rc = curvefs.Close(fd)
        if rc != 0:
            logger.info("close file fail! rc=%s" % rc)
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
            logger.info("rename file fail! rc=%s" % rc)
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
            #print "delete file fail! rc=%s" % rc
            logger.debug("delete file %s fail! rc=%s" % (filepath,rc))
#            logger.info("delete file fail! rc=%s" % rc)
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
            #print "delete dir fail! rc=%s" % rc
            logger.info("delete dir fail! rc=%s" % rc)
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
            #print "mkdir fail! rc=%s" % rc
            logger.info("mkdir fail! rc=%s" % rc)
            return rc
            #raise AssertionError
        else:
            return rc

def libcurve_uninit():
    rc = curvefs.UnInit()
    if rc != None:
        print "uninit  fail! rc=%s" % rc
        logger.debug("uninit file fail! rc=%s" % rc)
        return rc
        raise AssertionError
    else:
        return 0






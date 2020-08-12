#!/usr/bin/env python
# -*- coding: utf8 -*-

from config import config
from logger.logger import *
from lib import shell_operator
from keywords import base_operate
import time
import sys


def create_libcurve_file_with_normal_user():
    rc = base_operate.create_libcurve_file(config.file_name, config.user_name, size = config.size)
    if rc != 0:
        logger.error("create libcurve file fail. rc = %s" % rc)
        raise AssertionError
    else:
        return rc

def create_libcurve_file_when_parent_is_not_exist():
    rc = base_operate.create_libcurve_file(config.file_name_no_parent, config.user_name, size = config.size)
    return rc

def create_libcurve_file_when_parent_is_exist():
    rc = base_operate.create_libcurve_file(config.file_name_with_parent, config.user_name, size = config.size)
    if rc != 0:
        logger.error("create libcurve file fail. rc = %s" % rc)
        raise AssertionError
    else:
        return rc

def create_libcurve_file_less_than_10g_with_normal_user():
    rc = base_operate.create_libcurve_file(config.file_name, config.user_name, size = config.small_size)
    logger.info("create 1g file.")
    return rc

def create_libcurve_file_more_than_5t_with_normal_user():
    rc = base_operate.create_libcurve_file(config.file_name, config.user_name, size = config.big_size)
    logger.info("create 5t file.")
    return rc

def create_libcurve_file_with_normal_user_and_password():
    rc = base_operate.create_libcurve_file(config.file_name, config.user_name, config.size, config.root_password)
    if rc != 0:
        logger.error("create libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def create_libcurve_file_with_root_user_and_password():
    rc = base_operate.create_libcurve_file(config.file_name, config.root_name, config.size, config.root_password)
    if rc != 0:
        logger.error("create libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def create_libcurve_file_with_root_user_and_error_password():
    rc = base_operate.create_libcurve_file(config.file_name, config.root_name, config.size, config.root_error_password)
    if rc != 0:
        logger.info("create libcurve file with error root password. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def create_libcurve_file_with_root_user_and_no_password():
    rc = base_operate.create_libcurve_file(config.file_name, config.root_name, config.size, "")
    if rc != 0:
        logger.info("create libcurve file with error root password. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def create_libcurve_file_with_no_user():
    rc = base_operate.create_libcurve_file(config.file_name, "", config.size, config.root_password)
    logger.info("create libcurve file with no user.")
    return rc

def create_libcurve_dir_with_normal_user():
    rc = base_operate.create_libcurve_dir(config.dir_path, config.user_name)
    if rc != 0:
        logger.error("create libcurve dir fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def create_libcurve_dir_when_parent_is_not_exist():
    rc = base_operate.create_libcurve_dir(config.dir_path_no_parent, config.user_name)
    return rc

def create_libcurve_dir_when_parent_is_exist():
    rc = base_operate.create_libcurve_dir(config.dir_path_with_parent, config.user_name)
    if rc != 0:
        logger.error("create libcurve dir fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def create_libcurve_dir_with_normal_user_and_password():
    rc = base_operate.create_libcurve_dir(config.dir_path, config.user_name, config.root_password)
    if rc != 0:
        logger.error("create libcurve dir fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def create_libcurve_dir_with_root_user_and_password():
    rc = base_operate.create_libcurve_dir(config.dir_path, config.root_name, config.root_password)
    if rc != 0:
        logger.error("create libcurve dir fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def create_libcurve_dir_with_root_user_and_error_password():
    rc = base_operate.create_libcurve_dir(config.dir_path, config.root_name, config.root_error_password)
    if rc != 0:
        logger.info("create libcurve dir with error password fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def create_libcurve_dir_with_root_user_and_no_password():
    rc = base_operate.create_libcurve_dir(config.dir_path, config.root_name, "")
    if rc != 0:
        logger.info("create libcurve dir with error password fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def create_libcurve_dir_with_no_user():
    rc = base_operate.create_libcurve_dir(config.dir_path, "", config.root_password)
    logger.info("create libcurve dir with no user.")
    return rc

def open_libcurve_file_with_normal_user():
    rc = base_operate.open_libcurve_file(config.file_name, config.user_name)
    logger.info("open libcurve file with normal user.")
    if rc < 0:
        logger.error("open libcurve file with normal user fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc


def open_libcurve_file_with_normal_user_and_password():
    rc = base_operate.open_libcurve_file(config.file_name, config.user_name, config.root_password)
    if rc < 0:
        logger.error("open libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def open_libcurve_file_with_root_user_and_password():
    rc = base_operate.open_libcurve_file(config.file_name, config.root_name, config.root_password)
    if rc < 0:
        logger.error("open libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def open_libcurve_file_with_root_user_and_error_password():
    rc = base_operate.open_libcurve_file(config.file_name, config.root_name, config.root_error_password)
    if rc < 0:
        logger.info("open libcurve file with error password fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def open_libcurve_file_with_root_user_and_no_password():
    rc = base_operate.open_libcurve_file(config.file_name, config.root_name, "")
    if rc < 0:
        logger.info("open libcurve file with error password fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def open_libcurve_file_with_no_user():
    rc = base_operate.open_libcurve_file(config.file_name, "")
    logger.info("open libcurve file with no user.")
    return rc

def open_libcurve_file_with_error_user():
    rc = base_operate.open_libcurve_file(config.file_name, "test")
    logger.info("open libcurve file with error user.")
    return rc

def open_libcurve_file_with_error_user_and_root_password():
    rc = base_operate.open_libcurve_file(config.file_name, "test", config.root_password)
    logger.info("open libcurve file with error user and root password.")
    return rc

def extend_libcurve_file_with_normal_user():
    rc = base_operate.extend_libcurve_file(config.file_name, config.user_name, config.new_size)
    if rc < 0:
        logger.error("extend libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def extend_libcurve_file_with_normal_user_and_password():
    rc = base_operate.extend_libcurve_file(config.file_name, config.user_name, config.new_size, config.root_password)
    if rc < 0:
        logger.error("extend libcurve file fail. rc = %s" % rc)
        raise AssertionError
    else:
        return rc

def extend_libcurve_file_with_root_user_and_password():
    rc = base_operate.extend_libcurve_file(config.file_name, config.root_name, config.new_size, config.root_password)
    if rc < 0:
        logger.error("extend libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def extend_libcurve_file_with_root_user_and_error_password():
    rc = base_operate.extend_libcurve_file(config.file_name, config.root_name, config.new_size, config.root_error_password)
    if rc < 0:
        logger.info("extend libcurve file fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def extend_libcurve_file_with_root_user_and_no_password():
    rc = base_operate.extend_libcurve_file(config.file_name, config.root_name, config.new_size, "")
    if rc < 0:
        logger.info("extend libcurve file fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def extend_libcurve_file_with_no_user():
    rc = base_operate.extend_libcurve_file(config.file_name, "", config.new_size)
    logger.info("extend libcurve file with no user.")
    return rc

def extend_libcurve_file_with_error_user():
    rc = base_operate.extend_libcurve_file(config.file_name, "test", config.new_size)
    logger.info("extend libcurve file with error user.")
    return rc

def extend_libcurve_file_with_error_user_and_root_password():
    rc = base_operate.extend_libcurve_file(config.file_name, "test", config.new_size, config.root_password)
    logger.info("extend libcurve file with error user and root password.")
    return rc

def rename_libcurve_file_with_normal_user():
    rc = base_operate.rename_libcurve_file(config.old_name_with_parent, config.new_name_with_parent, config.user_name)
    if rc < 0:
        logger.error("rename libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def rename_libcurve_file_with_normal_user_and_password():
    rc = base_operate.rename_libcurve_file(config.old_name_with_parent, config.new_name_with_parent, config.user_name, config.pass_word)
    if rc < 0:
        logger.error("rename libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def rename_libcurve_file_with_root_user_and_password():
    rc = base_operate.rename_libcurve_file(config.old_name, config.new_name, config.root_name, config.root_password)
    if rc < 0:
        logger.error("rename libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def rename_libcurve_file_with_root_user_and_error_password():
    rc = base_operate.rename_libcurve_file(config.old_name, config.new_name, config.root_name, config.root_error_password)
    if rc < 0:
        logger.info("rename libcurve file fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def rename_libcurve_file_with_root_user_and_no_password():
    rc = base_operate.rename_libcurve_file(config.old_name, config.new_name, config.root_name, "")
    if rc < 0:
        logger.info("rename libcurve file fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def rename_libcurve_file_with_no_user():
    rc = base_operate.rename_libcurve_file(config.old_name, config.new_name, "")
    logger.info("rename libcurve file with no user.")
    return rc

def rename_libcurve_file_with_error_user():
    rc = base_operate.rename_libcurve_file(config.old_name, config.new_name, "test")
    logger.info("rename libcurve file with error user.")
    return rc

def rename_libcurve_file_with_error_user_and_root_password():
    rc = base_operate.rename_libcurve_file(config.old_name, config.new_name, "test", config.root_password)
    logger.info("rename libcurve file with error user and root password.")
    return rc

def delete_libcurve_file_with_normal_user():
    rc = base_operate.delete_libcurve_file(config.file_name, config.user_name)
    if rc < 0:
        #logger.error("delete libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def delete_libcurve_file_with_normal_user_and_password():
    rc = base_operate.delete_libcurve_file(config.file_name, config.user_name, config.root_password)
    if rc < 0:
        logger.debug("delete libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def delete_libcurve_file_with_root_user_and_password():
    rc = base_operate.delete_libcurve_file(config.file_name, config.root_name, config.root_password)
    if rc < 0:
        #logger.error("delete libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def delete_libcurve_file_with_root_user_and_error_password():
    rc = base_operate.delete_libcurve_file(config.file_name, config.root_name, config.root_error_password)
    if rc < 0:
        logger.info("delete libcurve file fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def delete_libcurve_file_with_root_user_and_no_password():
    rc = base_operate.delete_libcurve_file(config.file_name, config.root_name, "")
    if rc < 0:
        logger.info("delete libcurve file fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc


def delete_libcurve_file_with_no_user():
    rc = base_operate.delete_libcurve_file(config.file_name, "")
    logger.info("delete libcurve file with no user.")
    return rc

def delete_libcurve_file_with_error_user():
    rc = base_operate.delete_libcurve_file(config.file_name, "test")
    logger.info("delete libcurve file with error user.")
    return rc

def delete_libcurve_file_with_error_user_and_root_password():
    rc = base_operate.delete_libcurve_file(config.file_name, "test", config.root_password)
    logger.info("delete libcurve file with error user and root password.")
    return rc

def delete_libcurve_dir_with_normal_user():
    rc = base_operate.delete_libcurve_dir(config.dir_path, config.user_name)
    if rc < 0:
        logger.error("delete libcurve dir fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def delete_libcurve_dir_with_normal_user_and_password():
    rc = base_operate.delete_libcurve_dir(config.dir_path, config.user_name, config.root_password)
    if rc < 0:
        logger.error("delete libcurve dir fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def delete_libcurve_dir_with_root_user_and_password():
    rc = base_operate.delete_libcurve_dir(config.dir_path, config.root_name, config.root_password)
    if rc < 0:
        logger.info("delete libcurve dir fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def delete_libcurve_dir_with_root_user_and_error_password():
    rc = base_operate.delete_libcurve_dir(config.dir_path, config.root_name, config.root_error_password)
    if rc < 0:
        logger.info("delete libcurve dir fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def delete_libcurve_dir_with_root_user_and_no_password():
    rc = base_operate.delete_libcurve_dir(config.dir_path, config.root_name, "")
    if rc < 0:
        logger.info("delete libcurve dir fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def delete_libcurve_dir_with_no_user():
    rc = base_operate.delete_libcurve_dir(config.dir_path, "")
    logger.info("delete libcurve dir with no user.")
    return rc

def delete_libcurve_dir_with_error_user():
    rc = base_operate.delete_libcurve_dir(config.dir_path, "test")
    logger.info("delete libcurve dir with error user.")
    return rc

def delete_libcurve_dir_with_error_user_and_root_password():
    rc = base_operate.delete_libcurve_dir(config.dir_path, "test", config.root_password)
    logger.info("delete libcurve dir with error user and root password.")
    return rc

def statfs_libcurve_dir_with_normal_user():
    rc = base_operate.statfs_libcurve_dir(config.dir_path, config.user_name)
    if rc < 0:
        logger.error("statfs libcurve dir fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def statfs_libcurve_dir_with_normal_user_and_password():
    rc = base_operate.statfs_libcurve_dir(config.dir_path, config.user_name, config.root_password)
    if rc < 0:
        logger.error("statfs libcurve dir fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def statfs_libcurve_dir_with_root_user_and_password():
    rc = base_operate.statfs_libcurve_dir(config.dir_path, config.root_name, config.root_password)
    if rc < 0:
        logger.error("statfs libcurve dir fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def statfs_libcurve_dir_with_root_user_and_error_password():
    rc = base_operate.statfs_libcurve_dir(config.dir_path, config.root_name, config.root_error_password)
    if rc < 0:
        logger.info("statfs libcurve dir fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def statfs_libcurve_dir_with_root_user_and_no_password():
    rc = base_operate.statfs_libcurve_dir(config.dir_path, config.root_name, "")
    if rc < 0:
        logger.info("statfs libcurve dir fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def statfs_libcurve_dir_with_no_user():
    rc = base_operate.statfs_libcurve_dir(config.dir_path, "")
    logger.info("statfs libcurve dir with no user.")
    return rc

def statfs_libcurve_dir_with_error_user():
    rc = base_operate.statfs_libcurve_dir(config.dir_path, "test")
    logger.info("statfs libcurve dir with error user.")
    return rc

def statfs_libcurve_dir_with_error_user_and_root_password():
    rc = base_operate.statfs_libcurve_dir(config.dir_path, "test", config.root_password)
    logger.info("statfs libcurve dir with error user and root password.")
    return rc


def statfs_libcurve_file_with_normal_user():
    rc = base_operate.statfs_libcurve_file(config.file_name, config.user_name)
    if rc < 0:
        logger.error("statfs libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def statfs_libcurve_file_with_normal_user_and_password():
    rc = base_operate.statfs_libcurve_file(config.file_name, config.user_name, config.root_password)
    if rc < 0:
        logger.error("statfs libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def statfs_libcurve_file_with_root_user_and_password():
    rc = base_operate.statfs_libcurve_file(config.file_name, config.root_name, config.root_password)
    if rc < 0:
        logger.error("statfs libcurve file fail. rc = %s" % rc)
        return rc
        raise AssertionError
    else:
        return rc

def statfs_libcurve_file_with_root_user_and_error_password():
    rc = base_operate.statfs_libcurve_file(config.file_name, config.root_name, config.root_error_password)
    if rc < 0:
        logger.info("statfs libcurve file fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def statfs_libcurve_file_with_root_user_and_no_password():
    rc = base_operate.statfs_libcurve_file(config.file_name, config.root_name, "")
    if rc < 0:
        logger.info("statfs libcurve file fail. rc = %s" % rc)
        return rc
        #raise AssertionError
    else:
        return rc

def statfs_libcurve_file_with_no_user():
    rc = base_operate.statfs_libcurve_file(config.file_name, "")
    logger.info("statfs libcurve file with no user.")
    return rc

def statfs_libcurve_file_with_error_user():
    rc = base_operate.statfs_libcurve_file(config.file_name, "test")
    logger.info("statfs libcurve file with error user.")
    return rc

def statfs_libcurve_file_with_error_user_and_root_password():
    rc = base_operate.statfs_libcurve_file(config.file_name, "test", config.root_password)
    logger.info("statfs libcurve file with error user and root password.")
    return rc




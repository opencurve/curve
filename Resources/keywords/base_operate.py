#!/usr/bin/env python
# -*- coding: utf8 -*-

import os
import shlex
from config import config
from logger import logger
from lib import db_operator
from lib import shell_operator
from swig import swig_operate

#clean_db
def clean_db():
    try:
        cmd_list = ["DELETE FROM curve_logicalpool;", "DELETE FROM curve_copyset;", \
                "DELETE FROM curve_physicalpool;", "DELETE FROM curve_zone;", \
                "DELETE FROM curve_server;", "DELETE FROM curve_chunkserver;", \
                "DELETE FROM curve_session;"]
        for cmd in cmd_list:
            conn = db_operator.conn_db(config.db_host, config.db_port, config.db_user, config.db_pass, config.mds_db_name)
            db_operator.exec_sql(conn, cmd)
            logger.debug("clean db %s" %cmd)

    except Exception:
        logger.error("clean db fail.")
        raise

def drop_mds_table():
    try:
        cmd_list = ["DROP TABLE curve_logicalpool;", "DROP TABLE curve_copyset;", \
                    "DROP TABLE curve_physicalpool;", "DROP TABLE curve_zone;", \
                    "DROP TABLE curve_server;", "DROP TABLE curve_chunkserver;", \
                    "DROP TABLE curve_session;"]
        for cmd in cmd_list:
            conn = db_operator.conn_db(config.db_host, config.db_port, config.db_user, config.db_pass, config.mds_db_name)
            db_operator.exec_sql(conn, cmd)
            logger.debug("drop table %s" %cmd)
    except Exception:
        logger.error("drop db fail.")
        raise

def drop_snap_clone_table():
    try:
        cmd_list = ["DROP TABLE snapshot;", "DROP TABLE clone;"]
        for cmd in cmd_list:
            conn = db_operator.conn_db(config.db_host, config.db_port, config.db_user, config.db_pass, config.snap_db_name)
            db_operator.exec_sql(conn, cmd)
            logger.debug("drop table %s" %cmd)
    except Exception:
        logger.error("drop db fail.")
        raise

def mock_chunkserver_registe():
    try:
        mysql_cmd = ["INSERT INTO  curve_chunkserver VALUES (31, 'token1', 'nvme', '127.0.0.1', 8200, 0, 1, 0, 0, '/', 0, 0);",
                     "INSERT INTO  curve_chunkserver VALUES (32, 'token2', 'nvme', '127.0.0.1', 8201, 0, 2, 0, 0, '/', 0, 0);",
                     "INSERT INTO  curve_chunkserver VALUES (33, 'token3', 'nvme', '127.0.0.1', 8202, 0, 3, 0, 0, '/', 0, 0);"]
        for cmd in mysql_cmd:
            conn = db_operator.conn_db(config.db_host, config.db_port, config.db_user, config.db_pass, config.mds_db_name)
            db_operator.exec_sql(conn, cmd)
            logger.debug("insert db %s" % cmd)
    except Exception:
        logger.error("insert db fail. %s" % cmd)
        raise


def kill_process(process_name):
    grep_cmd = "ps -ef | grep %s | grep -v grep | awk '{print $2}' " %process_name
    pid = shell_operator.run_exec2(grep_cmd)
    logger.info("pid=%s" %pid)
    if pid:
        kill_cmd = "kill -9 %s" % pid
        ret_code = shell_operator.run_exec(kill_cmd)
        if ret_code == 0:
            return 0
        else:
            logger.error("kill process fail %s" % process_name )
    else:
        logger.debug("process %s not start." %process_name)

def stop_chunkserver():
    try:
        stop_cmd = "cd %s && %s" %(config.curve_workspace, config.chunkserver_stop_script)
        shell_operator.run_exec(stop_cmd)
    except Exception:
        logger.error("stop chunkserver fail.")
        raise

def chunkserver_log_create():
    try:
        mkdir_cmd = "mkdir -p %s + '/' + %s" %(config.curve_workspace, config.chunkserver_log_dir)
        shell_operator.run_exec(mkdir_cmd)
    except Exception:
        logger.error("mkdir for chunkserver dir fail.")
        raise

def clean_cs_data():
    try:
        clean_cs_data_dir0 = "rm -rf " + config.cs_0
        clean_cs_data_dir1 = "rm -rf " + config.cs_1
        clean_cs_data_dir2 = "rm -rf " + config.cs_2
        shell_operator.run_exec3(clean_cs_data_dir0)
        shell_operator.run_exec3(clean_cs_data_dir1)
        shell_operator.run_exec3(clean_cs_data_dir2)
    except Exception as e:
        logger.error("clean data fail.")
        raise e

def clean_etcd_data():
    shell_operator.run_exec("rm -rf default.etcd")

def start_chunkserver():
    try:
        start_cmd = "cd %s && %s" %(config.curve_workspace, config.chunkserver_start_script)
        shell_operator.run_exec(start_cmd)
    except Exception:
        logger.error("start chunkserver fail.")
        raise

def check_process_exsits(process_name):
    grep_cmd = "ps -ef | grep %s | grep -v grep | awk '{print $2}'  " % process_name
    pid = shell_operator.run_exec2(grep_cmd)
    logger.info("pid=%s" %pid)
    if pid:
        logger.debug("process %s exsits" % process_name)
        return 0
    else:
        logger.debug("process %s not exsits" % process_name)
        return -1

def start_mds():
    #try:
    start_cmd = "nohup " + config.mds_start +  " >> mds.log &"
        #start_cmd = config.mds_start + " " + config.mds_listen
    shell_operator.run_exec3(start_cmd)
    #except Exception as e:
        #logger.error("start mds fail.")
        #raise e

def start_snapshot_server():
    #try:
    start_cmd = "nohup " + config.snapshot_server_start + " -conf " + config.snapshot_clone_server_conf + " >> clone.log &"
    logger.info(start_cmd)
        #start_cmd = config.mds_start + " " + config.mds_listen
    shell_operator.run_exec3(start_cmd)
    #except Exception as e:
        #logger.error("start mds fail.")
        #raise e

def start_etcd():
    #try:
    start_cmd = "nohup " + "etcd >> etcd.log " + " &"
    logger.info(start_cmd)
        #start_cmd = config.mds_start + " " + config.mds_listen
    shell_operator.run_exec3(start_cmd)
    #except Exception as e:
        #logger.error("start mds fail.")
        #raise e

def create_physicalpool(cluster_map, mds_port, op): #need modify
    cmd = config.curvefs_tool + ' -cluster_map=%s' % cluster_map + ' -mds_port=%s' % mds_port + \
          ' -op=%s' % op
    ret_code = shell_operator.run_exec(cmd)
    if ret_code != 0:
        logger.error("create physicalpool failed. ret_code")
        raise AssertionError()
    else:
        return  ret_code


def create_logicalpool(copyset_num, mds_port, physicalpool_name, op):
    cmd = config.curvefs_tool + ' -copyset_num=%s' % copyset_num + ' -mds_port=%s' % mds_port+ \
          ' -physicalpool_name=%s' % physicalpool_name + ' -op=%s' % op
    ret_code = shell_operator.run_exec(cmd)
    print ret_code, cmd
    if ret_code != 0:
        logger.error("create logicalpool failed. ret_code")
        raise AssertionError()
    else:
        return ret_code

def run_libcurve_test(fake_mds, fake_chunkserver):
    cmd = config.libcurve_workflow + ' -fake_mds=%s' % fake_mds + ' -fake_chunkserver=%s' % fake_chunkserver
    ret_code = shell_operator.run_exec(cmd)
    print ret_code, cmd
    assert ret_code == 0

'''
def create_libcurve_file(file_name = "/vdisk_001", file_length = "10737418240", mds_addr = "127.0.0.1:6666"):
    cmd = config.createfile_tool + ' -file_name=%s' % file_name + ' -file_size=%s' %file_length + ' -mds_addr=%s' %mds_addr
    logger.debug("run start %s" %cmd)
    ret_code = shell_operator.run_exec(cmd)
    logger.debug("run end return code %s" %ret_code)
    assert ret_code == 0
'''

def create_libcurve_file(file_name = config.file_name, user_name = config.user_name, size = config.size, pass_word = config.pass_word):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_create(file_name, user_name, size, pass_word)
    if rc != 0:
        logger.info("create libcurve file fail. rc = %s" %rc)
        return rc

        #raise AssertionError
    else:

        return rc

def create_libcurve_dir(dir_path = config.dir_path, user_name = config.user_name, pass_word = config.pass_word):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_mkdir(dir_path, user_name, pass_word)
    if rc != 0:
        logger.info("create libcurve dir fail. rc = %s" %rc)
        return rc

        #raise AssertionError
    else:
        return rc

def open_libcurve_file(file_name =config.file_name, user_name = config.user_name, pass_word = config.pass_word):
    curvefs = swig_operate.LibCurve()
    fd = curvefs.libcurve_open(file_name, user_name, pass_word)
    logger.info("fd=%s" %fd)
    return fd


def extend_libcurve_file(file_name=config.file_name, user_name=config.user_name, new_size=config.new_size, pass_word=config.pass_word):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_extend(file_name, user_name, new_size, pass_word)

    if rc != 0:
        logger.info("extend libcurve file fail. rc = %s" %rc)
        return rc
        #raise AssertionError
    else:
        return rc


def rename_libcurve_file(old_name=config.old_name, new_name=config.new_name, user_name=config.user_name, pass_word=config.pass_word):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_rename(user_name, old_name, new_name, pass_word)

    if rc != 0:
        logger.info("rename libcurve file fail. rc = %s" %rc)
        return rc
        #raise AssertionError
    else:
        return rc


def statfs_libcurve_file(file_name=config.file_name, user_name=config.user_name, pass_word=config.pass_word):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_statfs(file_name, user_name, pass_word)

    if rc < 0:
        logger.info("stafs libcurve file fail. rc = %s" %rc)
        return rc
        #raise AssertionError
    else:
        return rc

def statfs_libcurve_dir(dir_path =config.dir_path, user_name = config.user_name, pass_word = config.pass_word):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_statfs(dir_path, user_name, pass_word)

    if rc < 0:
        logger.info("stafs libcurve dir fail. rc = %s" %rc)
        return rc
        #raise AssertionError
    else:
        return rc


def close_libcurve_file(fd):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_close(fd)
    if rc != 0:
        logger.info("close libcurve file fail. rc = %s" %rc)
        return rc
        #raise AssertionError
    else:

        return rc

def delete_libcurve_file(file_name = config.file_name, user_name = config.user_name, pass_word = config.pass_word):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_delete(file_name, user_name, pass_word)
    if rc != 0:
        #logger.error("delete libcurve file fail. rc = %s" %rc)
        return rc
        #raise AssertionError
    else:
        return rc

def delete_libcurve_dir(dir_path = config.dir_path, user_name = config.user_name, pass_word = config.pass_word):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_rmdir(dir_path, user_name, pass_word)
    if rc != 0:
        #logger.error("delete libcurve dir fail. rc = %s" %rc)
        return rc
        #raise AssertionError
    else:
        return rc

def write_libcurve_file(fd, buf=config.buf, offset=config.offset, length=config.length):
    curvefs = swig_operate.LibCurve()
    logger.info("fd=%s, buf=%s, offset=%s, length=%s" % (fd, buf, offset, length))
    rc = curvefs.libcurve_write(fd, buf, offset, length)
    if rc < 0:
        logger.error("write libcurve file fail. rc = %s" %rc)
        return rc
        raise AssertionError
    else:
        return rc

def read_libcurve_file(fd, buf="", offset=config.offset, length=config.length):
    curvefs = swig_operate.LibCurve()
    content = curvefs.libcurve_read(fd, buf, offset, length)
    return content


def cat_chunkserver_log(chunkserver_id):
    chunkserver_log = config.chunkserver_log_dir + chunkserver_id
    cmd = "cat %s" % chunkserver_log
    logger.debug("%scmdcmd" %cmd)
    if os.path.exists(chunkserver_log):
        shell_operator.run_exec(cmd)
    else:
        logger.error("chunkserver log not exists!")

def check_copyset_num(copyset_num):
    sql = "select * from curve_copyset;"
    conn = db_operator.conn_db(config.db_host, config.db_port, config.db_user, config.db_pass, config.mds_db_name)
    logicalpool_dbinof = db_operator.query_db(conn, sql)
    logger.info("logicalpool_dbinof = %s" % int(logicalpool_dbinof["rowcount"]))
    assert int(logicalpool_dbinof["rowcount"]) == int(copyset_num)

def get_buf():
    return config.buf















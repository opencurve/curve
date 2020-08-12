#!/usr/bin/env python
# -*- coding: utf8 -*-

import os
import shlex
from config import config
from logger.logger import *
from lib import shell_operator
from swig import swig_operate
from multiprocessing import Pool
# from curvefs_python import curvefs
import threading
import random
import time
import Queue
import types
import mythread

def mv_copyset_table():
    grep_cmd = "mv %s %s"%(config.mysql_log,config.curve_workspace)
    try:
        pid = shell_operator.run_exec2(grep_cmd)
    except Exception:
        logger.error("exec %s error" %grep_cmd)
        raise

def rm_copyset_table():
    grep_cmd = "rm mysql.log"
    try:
        pid = shell_operator.run_exec2(grep_cmd)
    except Exception:
        logger.error("exec %s error" %grep_cmd)
        raise

def get_copyset_scatterwith():
    cmd1 = 'sed -i "s/\\t//g" mysql.log'
    cmd2 = 'sed -i ":label;N;s/\\n//;b label" mysql.log'
    try:
        shell_operator.run_exec2(cmd1)
        shell_operator.run_exec2(cmd2)
    except Exception:
        logger.error("exec %s %s error"%(cmd1,cmd2))
        raise
    copyset_list = {}
    try:
        row = open("mysql.log").read()
    except Exception:
        logger.error("open mysql.log fail")
        raise
    row = row.split('|')
    for i in range(0, len(row)):
        if i % 2 != 0:
            copyset_list[(i + 1) / 2] = eval(row[i])
    logger.info("copset length is %d" % len(copyset_list))
    assert len(copyset_list) == config.copyset_num
    cs_list = []
    for key, value in copyset_list.items():
        cs = value
        for i in range(len(cs)):
            cs_list.append(cs[i])
    # print cs_list
    cs_list = list(set(cs_list))
#    logger.info("chunkserver list is %s"%cs_list)
    for cs in cs_list:
        cs_copyset_num = 0
        scatterwith = []
        for key, value in copyset_list.items():
            if cs in value:
                cs_copyset_num += 1
                for i in range(len(value)):
                    scatterwith.append(value[i])
        scatterwith = list(set(scatterwith))
        assert cs_copyset_num == (config.copyset_num*3/config.cs_num)
        logger.info("chunkserver %d copyset_num is %d \t,scatterwith is %d %s"%(cs,cs_copyset_num,len(scatterwith),scatterwith))
#        print "chunkserver %d ,scatterwith is %d" % (cs, len(scatterwith))


def kill_process(process_name):
    grep_cmd = "ps -ef | grep %s | grep -v grep | awk '{print $2}' " %process_name
    pids = shell_operator.run_exec2(grep_cmd)
    logger.info("pid=%s" %pids)
    if pids:
        for pid in pids:
            kill_cmd = "sudo kill -9 %s" % pid
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
        clean_cs_data_dir0 = "sudo rm -rf " + config.cs_0
        clean_cs_data_dir1 = "sudo rm -rf " + config.cs_1
        clean_cs_data_dir2 = "sudo rm -rf " + config.cs_2
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
    start_cmd = "nohup " + config.mds_start +  " 2>&1 | tee mds.log &"
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
    start_cmd = "nohup " + "etcd | tee etcd.log" + " &"
    logger.info(start_cmd)
        #start_cmd = config.mds_start + " " + config.mds_listen
    shell_operator.run_exec3(start_cmd)
    #except Exception as e:
        #logger.error("start mds fail.")
        #raise e


def stop_write():
    logger.info("set write_stopped = True")
    config.write_stopped = True

def create_physicalpool(cluster_map, mds_addr, op): #need modify
    cmd = config.curvefs_tool + ' -cluster_map=%s' % cluster_map + ' -mds_addr=%s' % mds_addr + \
          ' -op=%s' % op
    ret_code = shell_operator.run_exec(cmd)
    if ret_code != 0:
        logger.error("create physicalpool failed. ret_code is %d"%ret_code)
        raise AssertionError()
    else:
        return  ret_code


def create_logicalpool(cluster_map, mds_addr, op):
    cmd = config.curvefs_tool + ' -cluster_map=%s' % cluster_map + ' -mds_addr=%s' % mds_addr + \
          ' -op=%s' % op
    ret_code = shell_operator.run_exec(cmd)
    print ret_code, cmd
    if ret_code != 0:
        logger.error("create logicalpool failed. ret_code is %d"%ret_code)
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
        logger.error("create libcurve file %s fail. rc = %s" %(file_name,rc))
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
        logger.error("close libcurve file fail. rc = %s" %rc)
        return rc
        #raise AssertionError
    else:

        return rc

def delete_libcurve_file(file_name = config.file_name, user_name = config.user_name, pass_word = config.pass_word):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_delete(file_name, user_name, pass_word)
    if rc != 0:
        logger.debug("delete libcurve file %s fail. rc = %s" %(file_name,str(rc)))
        return rc
        #raise AssertionError
    else:
        logger.debug("delete libcurve file %s success. rc = %s" %(file_name,str(rc)))
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

def write_libcurve_file_error(fd, buf=config.buf, offset=config.offset, length=config.length):
    curvefs = swig_operate.LibCurve()
    ret = curvefs.libcurve_write(fd, buf, offset, length)
    logger.debug("write error,return id is %d"%ret)
    return ret

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

def get_buf():
    return config.buf

def loop_write_file(fd, num, offset, length):
    curvefs = swig_operate.LibCurve()
    i = 1
    buf_list = []
    while i < num + 1:
        if config.write_stopped == True:
            break
        buf_data = random.randint(1,9)
        buf = str(buf_data)*length
        logger.debug("begin write buf_data = %d"%buf_data)
        rc = curvefs.libcurve_write(fd, buf, offset, length)
        logger.debug("rc is %d" % rc)
        if rc > 0:
            buf_list.append(buf_data)
            i += 1
            offset += length
        else:
            raise AssertionError
    config.buf_list = buf_list
    config.write_stopped = False
def loop_write_file_noassert(fd, num, offset, length):
    curvefs = swig_operate.LibCurve()
    i = 1
    buf_list = []
    while i < num + 1:
        if config.write_stopped == True:
            break
        buf_data = random.randint(1,9)
        buf = str(buf_data)*length
        logger.debug("begin write buf_data = %d"%buf_data)
        rc = curvefs.libcurve_write(fd, buf, offset, length)
        logger.debug("rc is %d" % rc)
        if rc > 0:
            buf_list.append(buf_data)
            i += 1
            offset += length
    config.buf_list = buf_list
    config.write_stopped = False


def background_loop_write_file_noassert(fd,num=1000000,offset=config.offset, length=config.length):
    t = threading.Thread(target=loop_write_file_noassert, args=(fd,num,offset,length))
    t.start()
    return t

def background_loop_write_file(fd,num=1000000,offset=config.offset, length=config.length):
    t = mythread.runThread(loop_write_file,fd,num,offset,length)
    t.start()
    return t


def check_write_isalive(t):
    rc = t.is_alive()
    logger.debug("thread is %s"%t)
    logger.debug("rc is %s"%rc)
    assert rc == True

def wait_thread(t):
    time.sleep(1)
    t.join()

def check_loop_read(fd, offset=config.offset, length=config.length):
    curvefs = swig_operate.LibCurve()
    #logger.debug("buf_list is %s" % config.buf_list)
    i = 1
    for data in config.buf_list:
        buf = str(data)*length
        content = curvefs.libcurve_read(fd, buf, offset, length)
        assert buf == content,"buf is %s,content is %s"%(buf,content)
        logger.debug("read data is content %s"%content)
        i += 1
        offset += length

def loop_read_write_file_with_different_iosize(fd,offset=config.offset, length=config.length):
    curvefs = swig_operate.LibCurve()
    for i in range(1,10):
        buf = str(i)*length
        logger.debug("begin write buf_length = %d"%len(buf))
        rc = curvefs.libcurve_write(fd, buf, offset, length)
        assert rc == length
        content = curvefs.libcurve_read(fd, "", offset, length)
        assert buf == content,"buf is %s,content is %s"%(buf,content)
        offset += length
        length = length*2

def mult_process(func,num):
    pool = Pool(processes = num)
    results = []
    for  i in  xrange(num):
        results.append(pool.apply_async(globals().get(func),args=("/"+str(i),)))
        logger.debug("%s %s"%(func,str(i)))
    pool.close()
    pool.join()
    for result in results:
        logger.debug("get is %d"%(result.get()))
        assert result.get() == 0


def mult_thread(func,num,pre_path="/"):
#    pool = Pool(processes = num)
    thread = []
    results = []
    if pre_path != "/":
        path = pre_path + "/"
        curvefs = swig_operate.LibCurve()
        rc = curvefs.libcurve_statfs(pre_path, user_name=config.user_name, pass_word=config.pass_word)
        logger.debug("path is %s" % path)
        parentid = rc.id
    else:
        path = pre_path
        parentid = 0
    for  i in  xrange(num):
#        results.append(pool.apply_async(globals().get(func),args=("/"+str(i),)))
        filename = path + str(i)
        logger.debug("filename is %s"%filename)
        t = mythread.runThread(globals().get(func),filename)
        thread.append(t)
        logger.debug("%s %s"%(func,str(i)))
    for t in thread:
        t.start()
    if str(func) == "statfs_libcurve_file":
        for t in thread:
            result = t.get_result()
            assert result.length == 10737418240, "file length is %d"%(result.length)
            assert result.parentid == parentid, "file parentid is %d,pre id is %d"%(result.parentid,parentid)
            assert result.filetype == 1, "file filetype is %d" % (result.filetype)
    elif str(func) == "open_libcurve_file":
        for t in thread:
            results.append(t.get_result())
            assert t.get_result() != None, "open file fd is %d"%t.get_result()
        return results
    elif str(func) == "create_libcurve_file":
        for t in thread:
            rc = t.get_result()
            logger.debug("get result is %d"%rc)
            if rc == -1:
                logger.error("file exist")
            else:
                assert rc == 0,"result is %s"%str(rc)
    elif str(func) == "delete_libcurve_file":
        for t in thread:
            rc = t.get_result()
            logger.debug("get result is %d"%rc)
            if rc == -6:
                logger.error("file not exist")
            else:
                assert rc == 0,"result is %s"%str(rc)
    else:
        for t in thread:
            rc = t.get_result()
            logger.debug("get result is %d"%rc)
            assert rc == 0,"result is %s"%str(rc)


def mult_thread_close_file(fd,num):
#    pool = Pool(processes = num)
    thread = []
    for  i in  fd:
#        results.append(pool.apply_async(globals().get(func),args=("/"+str(i),)))
        t = mythread.runThread(close_libcurve_file,i)
        thread.append(t)
        logger.debug("%s %s"%(close_libcurve_file,i))
    for t in thread:
        t.start()
    for t in thread:
        logger.debug("get result is %d"%t.get_result())
        assert t.get_result() == 0


def create_multi_dir(depth,user_name=config.user_name, pass_word=config.pass_word):
    curvefs = swig_operate.LibCurve()
    pre_path = ""
    for i in range(depth):
        dir_path = pre_path + "/" + str(i)
        rc = curvefs.libcurve_mkdir(dir_path, user_name, pass_word)
        logger.debug("create dir dir_path %s"%dir_path)
        assert rc == 0,"crate dir %s fail ,rc is %d"%(dir_path,rc)
        pre_path = dir_path

def delete_multi_dir(depth,user_name=config.user_name, pass_word=config.pass_word):
    curvefs = swig_operate.LibCurve()
    last_path = ""
    for i in range(depth):
        last_path = last_path + "/" + str(i)
    dir_path = last_path
    for i in range(depth):
        rc = curvefs.libcurve_rmdir(dir_path, user_name, pass_word)
        logger.debug("rm dir dir_path %s" % dir_path)
        assert rc == 0, "rm dir %s fail ,rc is %d" % (dir_path, rc)
        dir_path = last_path.rsplit("/", 1)[0]
        last_path = dir_path

def test():
    logger.info("pass,need to add later")

def test_kill_one_chunkserver():
    id = random.randint(0,2)
    process_name = "chunkserver.conf.%d"%id
    logger.info("kill chunkserver %d"%id)
    kill_process(process_name)

def thrasher(fd):
    actions = []
    actions.append((extend_libcurve_file,1.0,))
    actions.append((test,0.5,))
    curvefs = swig_operate.LibCurve()
#    actions.append((create_libcurve_file,1.0,))
    for i in range(1, 10):
        total = sum([y for (x, y) in actions])
        logger.debug("total %s" %(total))
        val = random.uniform(0, total)
        logger.debug("val is %s" % (val))
        for (action, prob) in actions:
            logger.info("running time %s " % (i))
            if val < prob:
                if  action == extend_libcurve_file:
                    new_size = 10737418240 * i
                    logger.debug("running action %s" % (action))
                    rc = action(new_size=new_size)
                    assert rc == 0
                    rc = curvefs.libcurve_statfs(file_name=config.file_name, user_name=config.user_name, \
                                                 pass_word=config.pass_word)
                    assert rc.length == new_size,"get file length is %d"%rc.length
                else:
                    action()
            val -= prob

#!/usr/bin/env python
# -*- coding: utf8 -*-

import subprocess

from config import config

from logger import logger


def run_exec(cmd, lock=None):
    '''
    执行指定的命令，在当前进程执行，判断命令是否执行成功，执行失败抛异常

    @param log_id 日志打印的id，暂无
    @param cmd 要执行的命令
    @param lock 参数lock是一个锁，如果不为None，则命令执行过程中是要加锁的
    @return: None

    '''
    logger.debug("run start: %s" % cmd)
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
        #  logger.debug("run result: %s " % out_msg.strip())
        logger.debug("run end: %s" % cmd)
        return 0
    else:
        #  logger.debug("run result: %s " % err_msg.strip())
        logger.error("run end: %s" % cmd)
        return -1

def run_exec2(cmd, lock=None):
    '''
    执行指定的命令,在当前进程执行，返回输出结果，执行失败为

    @param log_id 日志打印的id，暂无
    @param cmd 要执行的命令
    @param lock 参数lock是一个锁，如果不为None，则命令执行过程中是要加锁的
    @return: 命令输出结果

    '''
    logger.debug("run start: %s" % cmd)
    out_msg = ''
    try:
        if lock:
            lock.acquire()
        out_msg = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except Exception as e:
        logger.error("catch error: %s" % out_msg)
        raise e
        #  run_err_msg = traceback.format_exc()

        #  return False
    finally:
        if lock:
            lock.release()

    logger.debug("run result: %s " % out_msg)
    logger.debug("run end: %s " % cmd)
    return out_msg.strip()


def run_exec3(cmd, lock=None):
    '''
    执行指定的命令，在子进程中异步执行

    @param log_id 日志打印的id， 暂无
    @param cmd 要执行的命令
    @param lock 参数lock是一个锁，如果不为None，则命令执行过程中是要加锁的
    @return: None

    '''
    logger.debug("run start: %s" % cmd)
    try:
        if lock:
            lock.acquire()
        p = subprocess.Popen(cmd, shell=True)
        ret_code = p.returncode
    except Exception as e:
        raise e
    finally:
        if lock:
            lock.release()
    if ret_code:
        logger.error("run cmd fail %s, ret_code=%s" % (cmd, ret_code))


def gen_cmd(ori_cmd, sudo_flag=config.sudo_flag, nfit_ssh_unit=None):
    '''
    TODO: Docstring for gen_cmd.

    @param ori_cmd 原始命令
    @param sudo_flag
    @param nfit_ssh_unit ssh信息
    @return: cmd 包装后的命令

    '''
    if nfit_ssh_unit is None:
        cmd = gen_sudo_cmd(ori_cmd, sudo_flag)
    else:
        cmd = gen_remote_cmd(nfit_ssh_unit._user,
                             nfit_ssh_unit._host_ip,
                             nfit_ssh_unit._port,
                             nfit_ssh_unit._ssh_key,
                             ori_cmd,
                             sudo_flag)
    return cmd


def gen_sudo_cmd(ori_cmd, sudo_flag=False, sudo_way=config.sudo_way):
    '''
    TODO: Docstring for gen_cmd.

    @param ori_cmd 原始命令
    @param sudo_flag sudo标志，判断是否需要sudo
    @param sudo_way sudo方式，如"sudo su - nbs -c"
    @return: sudo包装后的cmd

    '''
    if sudo_flag == False:
        cmd = ori_cmd
    else:
        cmd = '%s %s' % (sudo_way, ori_cmd)
    return cmd


def gen_remote_cmd(user, host_ip, port, ssh_key, ori_cmd, sudo_flag=False, sudo_way=config.sudo_way):
    '''
    将本地命令整合成一条ssh远程命令

    @param user 目标机器登陆用户
    @param host_ip 目标机器ip
    @param port 目标机器端口
    @param ssh_key 登陆目标机器的私钥
    @param ori_cmd 执行的命令
    @param sudo_user 执行命令的sudo账户，如nbs
    @param sudo_way sudo方式，如sudo -iu
    @return: 包装后远程执行的命令

    '''
    if sudo_flag is False:
        cmd = 'ssh %s@%s -p%s -i %s -o StrictHostKeyChecking=no "%s"' % (user, host_ip, port, ssh_key, ori_cmd)
    else:
        # 直接sudo su - nbs -c "cmd"，这样有可能会有权限限制，所以使用另外一种方式echo "cmd" | sudo su - nbs
        # cmd = 'ssh %s@%s -p%s -i %s "%s" | %s %s' % (user, host_ip, port, ssh_key, ori_cmd, sudo_way, sudo_user)
        cmd = 'ssh %s@%s -p%s -i %s -o StrictHostKeyChecking=no "%s %s" ' % (user, host_ip, port, ssh_key, sudo_way, ori_cmd)
    return cmd
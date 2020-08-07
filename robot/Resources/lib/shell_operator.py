#!/usr/bin/env python
# -*- coding: utf8 -*-

import subprocess

from config import config

from logger.logger import *
import paramiko
import time

def run_exec(cmd, lock=None):
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
        logger.debug("run end: %s" % cmd)
        return -1

def run_exec2(cmd, lock=None):
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
    if sudo_flag == False:
        cmd = ori_cmd
    else:
        cmd = '%s %s' % (sudo_way, ori_cmd)
    return cmd


def gen_remote_cmd(user, host_ip, port, ssh_key, ori_cmd, sudo_flag=False, sudo_way=config.sudo_way):
    if sudo_flag is False:
        cmd = 'ssh %s@%s -p%s -i %s -o StrictHostKeyChecking=no "%s"' % (user, host_ip, port, ssh_key, ori_cmd)
    else:
        cmd = 'ssh %s@%s -p%s -i %s -o StrictHostKeyChecking=no "%s %s" ' % (user, host_ip, port, ssh_key, sudo_way, ori_cmd)
    return cmd

def create_ssh_connect(host,port,user):
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    transport = paramiko.Transport((host,port))
    transport.banner_timeout = 60
    key = paramiko.RSAKey.from_private_key_file(config.pravie_key_path)
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host,port=port,username=user,timeout=100,pkey=key)
    return ssh

def ssh_exec(con, cmd,log=logger):
    log.info('ssh exec cmd %s', cmd)
    stdin, stdout, stderr = con.exec_command(cmd, timeout=120)
    return_code = stdout.channel.recv_exit_status()
    return stdin,stdout.readlines(),stderr.readlines(),return_code

def ssh_background_exec(con, cmd):
    logger.info('ssh exec cmd %s', cmd)
    transport = con.get_transport()
    channel = transport.open_session()
    channel.exec_command(cmd)

def ssh_background_exec2(con, cmd):
    logger.info('ssh exec cmd %s', cmd)
    chan = con.invoke_shell()
    chan.send(cmd + ' \n')
    time.sleep(1)


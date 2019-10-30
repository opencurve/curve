#!/usr/bin/env python
# -*- coding:UTF-8

from jinja2 import Environment, FileSystemLoader
import json
from multiprocessing import cpu_count
import os
import shlex
from subprocess import Popen, PIPE
from threading import Timer
import time
import unittest


import libvirt
import paramiko
import rados
import rbd

# 需要先在物理机上配置好网桥，名称bri，配置dhcp服务，并修改如下相关配置项
CEPH_CONF = "/etc/ceph/ceph.conf"
MON_HOSTS = ["10.182.30.27", "10.182.30.28", "10.182.30.29"]
REUSE_RBD_DISK = False  # True表示测试前不创建rbd卷（使用已有的），跑完不清理rbd卷，False则相反
RBD_POOL = "rbd"
RBD_VM_IMG = "rbd_sys_disk"  # 系统盘
RBD_VM_IMG_SIZE = 4<<30  # GB，需要比文件镜像大
RBD_VOL1 = "rbd_logic_disk"  # 云盘
RBD_VOL1_SIZE = 1<<30  # GB，至少1G
LOCAL_IMG = "/mnt/centos_7.5.raw"  # 必须是raw格式镜像，不能qcow2
QEMU_PROC_NAME = "qemu-system-x86_64"
NEBD_SERVER_EXEC_FILE = "/usr/bin/nebd-server"
METADATA_DIR = "/var/run/nebd-server/"
CEPH_CLIENT_OLD_PACKAGE = "0.94.6+netease.1.2-0"
CEPH_CLIENT_NEW_PACKAGE = "0.94.6+netease.1.5-2"
CEPH_CLIENT_REAL_VERSION = {  # 通过admin socket version命令获取的版本号
    CEPH_CLIENT_OLD_PACKAGE: "0.94.6-11-g61fb955",
    CEPH_CLIENT_NEW_PACKAGE: "0.94.6+netease.1.5-2"
}
CEPH_CLIENT_ASOK_DIR = "/var/run/ceph/guests/"


VM_SSH_KEY = "./id_rsa"  # 免密ssh到vm私钥（root用户600权限）
VM_HOST_IP = "192.168.10.10"  # vm的ip地址，最好配置为静态ip
UUID1 = "5d1289be-50e1-47b7-86de-1de0ff16a9d4"
UUID2 = "b5fdf3de-320c-41cf-80b2-acb794078012"
MAX_CPUS = cpu_count()

LIBVIRT_CONN = None
JJ_ENV = None



def run(cmd, timeout=5, ignore=''):
    print 'going to run cmd: %s' % cmd
    proc = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    timer = Timer(timeout, proc.kill)
    try:
        timer.start()
        stdout, stderr = proc.communicate()
        if stderr:
            if ignore == '' or ignore not in stderr:
                raise OSError(stderr)
        return stdout
    except Exception as ex:
        print ex
        raise ex
    finally:
        timer.cancel()


def create_ssh_connect(host, port=22, timeout=3):
    key = paramiko.RSAKey.from_private_key_file(VM_SSH_KEY)
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    while timeout > 0:
        timeout -= 1
        try:
            ssh.connect(host, username='root', port=port, pkey=key, timeout=1)
            return ssh
        except paramiko.ssh_exception.NoValidConnectionsError as exc:
            print "ssh to %s failed: %s" % (host, str(exc))
            time.sleep(1)
        except Exception as exc:
            print "ssh to %s failed: %s" % (host, str(exc))
    return None


def ssh_exec(conn, cmd, timeout=5):
    print "going to ssh_exec cmd %s" % cmd
    stdin, stdout, stderr = conn.exec_command(cmd, timeout=timeout)
    return (stdin, stdout.readlines(), stderr.readlines())


class VMIntegTest(unittest.TestCase):
    '''热升级功能集成测试类'''

    def setUp(self):
        clear_all_vms()

    def _clear_all_metadatafiles(self):
        print 'going to clear all metadata files...'
        fs = os.listdir(METADATA_DIR)
        for f in fs:
            if len(f) == len(UUID1):
                os.remove(os.path.join(METADATA_DIR, f))

    def _killall_nebd_servers(self):
        cmd_killall = 'killall -9 %s' % os.path.basename(NEBD_SERVER_EXEC_FILE)
        try:
            run(cmd_killall)
        except OSError:
            pass

    def tearDown(self):
        clear_all_vms()
        self._killall_nebd_servers()
        self._clear_all_metadatafiles()

    def test_create_destroy_vm(self):
        cpuset = '0-%d' % (MAX_CPUS - 1)
        if MAX_CPUS >= 2:
            cpuset = '0-%d' % (MAX_CPUS / 2 - 1)
        dominfo = {"uuid": UUID1, "cpuset": cpuset,
                   "name": "test_create_destroy_vm",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        dom = self._create_vm(dominfo)

        if dom.isActive():
            # 检查nebd-server进程的cpu亲和性是否与qemu进程一致
            time.sleep(3)
            nebd_server_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                                       NEBD_SERVER_EXEC_FILE)
            self.assertTrue(nebd_server_pid > 0)
            ns_cpu_affinity = run('taskset -pc %d' % nebd_server_pid)
            qemu_pid = self._get_pid_by_cmdline(dominfo['uuid'], QEMU_PROC_NAME)
            self.assertTrue(qemu_pid > 0)
            qemu_cpu_affinity = run('taskset -pc %d' % qemu_pid)
            self.assertEqual(ns_cpu_affinity.split(':')[-1].strip(),
                             qemu_cpu_affinity.split(':')[-1].strip())
            self._destroy_vm(dominfo)
        else:
            self.assertFalse("Domain creates failed")

    def test_shutdown_vm(self):
        rbd_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_shutdown_rbd_vm",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        local_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_shutdown_local_vm",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG
        }
        dominfos = [rbd_vm, local_vm]

        for dominfo in dominfos:
            dom = self._create_vm(dominfo)
            if dom.isActive():
                self._destroy_vm(dominfo, shutdown=True, timeout=120)
            else:
                self.assertFalse("Domain creates failed")

    def test_reboot_vm(self):
        rbd_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_reboot_rbd_vm",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        local_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_reboot_local_vm",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG
        }
        dominfos = [rbd_vm, local_vm]

        for dominfo in dominfos:
            dom = self._create_vm(dominfo)
            if not dom.isActive():
                self.assertFalse("Domain creates failed")
            # 等待虚拟机操作系统启动
            ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
            self.assertTrue(ssh is not None)
            # 关机
            self._destroy_vm(dominfo, shutdown=True, timeout=120)
            # 再启动（模拟重启）
            dom = self._create_vm(dominfo)
            if not dom.isActive():
                self.assertFalse("Domain creates failed")
            # 等待虚拟机操作系统启动
            ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
            self.assertTrue(ssh is not None)
            # 关机
            self._destroy_vm(dominfo)

    def test_reboot_vm_with_exist_vnet(self):
        tap_name = "tap0"
        rbd_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_reboot_rbd_vm_with_exist_vnet",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS,
                   "tap": tap_name
        }
        local_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_reboot_local_vm_with_exist_vnet",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG,
                   "tap": tap_name
        }
        dominfos = [rbd_vm, local_vm]

        for dominfo in dominfos:
            # 删除tap设备
            cmd_tap = ' '.join(['/usr/sbin/tunctl', '-d', tap_name])
            out = run(cmd_tap)
            # 创建tap设备
            cmd_tap = ' '.join(['/usr/sbin/tunctl', '-t', tap_name])
            out = run(cmd_tap)
            self.assertTrue(tap_name in out and 'persistent' in out)
            dom = self._create_vm(dominfo)
            if not dom.isActive():
                self.assertFalse("Domain creates failed")
            # 等待虚拟机操作系统启动
            ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
            self.assertTrue(ssh is not None)
            # 关机
            self._destroy_vm(dominfo)
            # 从网桥上移除tap设备
            cmd_brctl = ' '.join(['/sbin/brctl', 'delif', 'bri', tap_name])
            out = run(cmd_brctl)
            self.assertFalse(out)
            # 再启动（模拟重启）
            dom = self._create_vm(dominfo)
            if not dom.isActive():
                self.assertFalse("Domain creates failed")
            # 等待虚拟机操作系统启动
            ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
            self.assertTrue(ssh is not None)
            # 关机
            self._destroy_vm(dominfo)
            # 从网桥上移除tap设备
            cmd_brctl = ' '.join(['/sbin/brctl', 'delif', 'bri', tap_name])
            out = run(cmd_brctl)
            self.assertFalse(out)
            # 删除tap设备
            cmd_tap = ' '.join(['/usr/sbin/tunctl', '-d', tap_name])
            out = run(cmd_tap)
            self.assertTrue(tap_name in out and 'nonpersistent' in out)

    def test_poweroff_vm_inside(self):
        rbd_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_poweroff_rbd_vm_inside",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        local_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_poweroff_local_vm_inside",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG
        }
        dominfos = [rbd_vm, local_vm]

        for dominfo in dominfos:
            dom = self._create_vm(dominfo)
            if not dom.isActive():
                self.assertFalse("Domain creates failed")

            ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
            self.assertTrue(ssh is not None)
            ssh_exec(ssh, 'poweroff')

            is_poweroff = False
            poweroff_timeout = 30
            while poweroff_timeout > 0:
                try:
                    dom = LIBVIRT_CONN.lookupByUUIDString(dominfo['uuid'])
                    if dom.info()[0] == libvirt.VIR_DOMAIN_SHUTOFF:
                        is_poweroff = True
                        break
                    poweroff_timeout -= 1
                    print 'sleep 1s'
                    time.sleep(1)
                except libvirt.libvirtError:
                    is_poweroff = True
                    break
            self.assertTrue(is_poweroff)

    def test_reboot_vm_inside(self):
        rbd_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_reboot_rbd_vm_inside",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        local_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_reboot_local_vm_inside",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG
        }
        dominfos = [rbd_vm, local_vm]

        for dominfo in dominfos:
            dom = self._create_vm(dominfo)
            if not dom.isActive():
                self.assertFalse("Domain creates failed")

            ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
            self.assertTrue(ssh is not None)
            ssh_exec(ssh, '>/var/log/wtmp')
            ssh_exec(ssh, 'reboot')

            is_reboot = False
            reboot_timeout = 30
            while reboot_timeout > 0:
                ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
                if ssh is not None:
                    _, out, err = ssh_exec(ssh, 'last reboot|head -1')
                    print out, err
                    if 'reboot' in out[0]:
                        is_reboot = True
                        break
                reboot_timeout -= 1
                print 'sleep 1s'
                time.sleep(1)
            self.assertTrue(is_reboot)
            self._destroy_vm(dominfo)

    def test_pause_unpause_vm(self):
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_pause_unpause_vm",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        dom = self._create_vm(dominfo)

        if dom.isActive():
            ret = dom.suspend()
            self.assertTrue(ret >= 0)
        else:
            self.assertFalse("Domain creates failed")

        self.assertEqual(3, dom.info()[0])
        ret = dom.resume()
        self.assertTrue(ret >= 0)
        self.assertEqual(1, dom.info()[0])
        ret = dom.destroy()
        self.assertTrue(ret >= 0)

    def test_save_restore_vm(self):
        rbd_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_save_restore_rbd_vm_kill_part2",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        rbd_vm1 = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_save_restore_rbd_vm",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        local_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_save_restore_local_vm",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG
        }
        dominfos = [rbd_vm, rbd_vm1, local_vm]

        # 依次测试rbd、rbd、local系统盘
        for dominfo in dominfos:
            dom = self._create_vm(dominfo, transient=False)
            part2_pid = -1
            if dom.isActive():
                part2_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                                     NEBD_SERVER_EXEC_FILE)
                self.assertTrue(part2_pid > 0)
                ret = dom.save('/tmp/' + dominfo['uuid'])
                self.assertTrue(ret >= 0)
            else:
                self.assertFalse("Domain creates failed")

            self.assertEqual(libvirt.VIR_DOMAIN_SHUTOFF, dom.info()[0])
            # rbd系统盘第一个用例测试kill part2场景下restore vm
            if dominfos.index(dominfo) == 0:
                try:
                    os.kill(part2_pid, 9)
                except OSError as exc:
                    if "No such process" in str(exc):
                        pass
                time.sleep(1)
                self.assertFalse(self._check_proc_running(part2_pid,
                                    dominfo['uuid'], NEBD_SERVER_EXEC_FILE))
            ret = LIBVIRT_CONN.restore('/tmp/' + dominfo['uuid'])
            self.assertTrue(ret >= 0)
            self.assertEqual(1, dom.info()[0])
            ret = dom.undefine()
            self.assertTrue(ret >= 0)
            ret = dom.destroy()
            self.assertTrue(ret >= 0)
            os.remove('/tmp/' + dominfo['uuid'])

    # 创建vm，并挂卸载卷，设置卷QoS
    def _attach_detach_disk_on_vm(self, dominfo, diskinfo):
        template = JJ_ENV.get_template('domain_xml.j2')
        domxml = template.render(dominfo)
        ret = LIBVIRT_CONN.defineXML(domxml)
        self.assertTrue(ret >= 0)
        dom = LIBVIRT_CONN.lookupByUUIDString(dominfo['uuid'])

        # 离线挂载卷测试
        template = JJ_ENV.get_template('disk_xml.j2')
        diskxml = template.render(diskinfo)
        ret = dom.attachDeviceFlags(diskxml, libvirt.VIR_DOMAIN_AFFECT_CONFIG)
        self.assertTrue(ret >= 0)
        xml = dom.XMLDesc(0)
        self.assertTrue(diskinfo['disk_name'] in xml)

        # 离线设置卷QoS
        params = {'total_bytes_sec': 1024000, 'total_iops_sec': 100}
        flag = libvirt.VIR_DOMAIN_AFFECT_CONFIG
        ret = dom.setBlockIoTune(diskinfo['target_dev'], params, flag)
        self.assertTrue(ret >= 0)
        xml = dom.XMLDesc(0)
        self.assertTrue(params.keys()[0] in xml
                        and str(params.values()[0]) in xml)

        # 离线卸载卷测试
        ret = dom.detachDeviceFlags(diskxml, libvirt.VIR_DOMAIN_AFFECT_CONFIG)
        self.assertTrue(ret >= 0)
        xml = dom.XMLDesc(0)
        self.assertFalse(diskinfo['disk_name'] in xml)

        # 在线挂载卷测试
        ret = dom.create()
        self.assertTrue(ret >= 0)

        timeout = 30
        is_active = False
        while timeout > 0:
            try:
                dom = LIBVIRT_CONN.lookupByUUIDString(dominfo['uuid'])
            except:
                self.assertFalse("Look up domain by uuid failed")
            if dom.isActive():
                is_active = True
                break
            timeout -= 1
            print 'sleep 1s'
            time.sleep(1)

        if not is_active:
            self.assertFalse("Domain creates failed")
        time.sleep(3)
        ret = dom.attachDeviceFlags(diskxml, libvirt.VIR_DOMAIN_AFFECT_LIVE)
        self.assertTrue(ret >= 0)
        xml = dom.XMLDesc(0)
        self.assertTrue(diskinfo['disk_name'] in xml)

        # 在线设置卷QoS
        params = {'write_bytes_sec': 1024000, 'read_bytes_sec': 2048000}
        flag = libvirt.VIR_DOMAIN_AFFECT_LIVE
        ret = dom.setBlockIoTune(diskinfo['target_dev'], params, flag)
        self.assertTrue(ret >= 0)
        xml = dom.XMLDesc(0)
        self.assertTrue(params.keys()[0] in xml
                        and str(params.values()[0]) in xml)

        # 在线卸载卷测试
        time.sleep(3)
        ret = dom.detachDeviceFlags(diskxml, libvirt.VIR_DOMAIN_AFFECT_LIVE)
        self.assertTrue(ret >= 0)
        xml = dom.XMLDesc(0)
        self.assertFalse(diskinfo['disk_name'] in xml)

        ret = dom.undefine()
        self.assertTrue(ret >= 0)
        ret = dom.destroy()
        self.assertTrue(ret >= 0)

    # vm系统盘使用rbd卷，挂卸载rbd卷测试
    def test_attach_detach_rbd_disk_on_rbd_vm(self):
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_attach_detach_rbd_disk_on_rbd_vm",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        diskinfo = {"protocol": "rbd",
                    "disk_name": os.path.join(RBD_POOL, RBD_VOL1),
                    "mon_hosts": MON_HOSTS,
                    "target_dev": "vdc"
        }
        self._attach_detach_disk_on_vm(dominfo, diskinfo)

    # vm系统盘使用本地qcow2 file，挂卸载rbd卷测试
    def test_attach_detach_rbd_disk_on_local_vm(self):
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_attach_detach_rbd_disk_on_local_vm",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG
        }
        diskinfo = {"protocol": "rbd",
                    "disk_name": os.path.join(RBD_POOL, RBD_VOL1),
                    "mon_hosts": MON_HOSTS,
                    "target_dev": "vdc"
        }
        self._attach_detach_disk_on_vm(dominfo, diskinfo)

    # vm系统盘使用rbd卷，挂卸载cbd卷测试
    def test_attach_detach_cbd_disk_on_rbd_vm(self):
        # TODO
        pass

    # vm系统盘使用本地qcow2 file，挂卸载rbd卷测试
    def test_attach_detach_cbd_disk_on_local_vm(self):
        # TODO
        pass

    def _destroy_vm(self, dominfo, shutdown=False, transient=True, timeout=5):
        dom = LIBVIRT_CONN.lookupByUUIDString(dominfo['uuid'])
        shutdown_timeout = timeout
        if shutdown:
            # 等待vm完全启动，通过尝试ssh来判断
            ssh_timeout = timeout * 2 / 3
            shutdown_timeout = timeout - ssh_timeout
            ssh = create_ssh_connect(VM_HOST_IP, timeout=ssh_timeout)
            ret = dom.shutdown()
        else:
            ret = dom.destroy()
        self.assertTrue(ret >= 0)

        is_destroy = False
        while shutdown_timeout > 0:
            try:
                dom = LIBVIRT_CONN.lookupByUUIDString(dominfo['uuid'])
                if dom.info()[0] == libvirt.VIR_DOMAIN_SHUTOFF:
                    is_destroy = True
                    break
                shutdown_timeout -= 1
                print 'sleep 1s'
                time.sleep(1)
            except libvirt.libvirtError:
                is_destroy = True
                break
        self.assertTrue(is_destroy)

        if not transient:
            ret = dom.undefine()
            self.assertTrue(ret >= 0)

    # 创建虚拟机
    def _create_vm(self, dominfo, transient=True,
                   xml_j2='domain_xml.j2', timeout=30):
        template = JJ_ENV.get_template(xml_j2)
        domxml = template.render(dominfo)
        if transient:  # 非持久化vm
            ret = LIBVIRT_CONN.createXML(domxml)
        else:  # 持久化vm
            ret = LIBVIRT_CONN.defineXML(domxml)
        self.assertTrue(ret >= 0)

        dom = LIBVIRT_CONN.lookupByUUIDString(dominfo['uuid'])
        if not transient:
            ret = dom.create()
        self.assertTrue(ret >= 0)

        is_active = False
        while timeout > 0:
            try:
                dom = LIBVIRT_CONN.lookupByUUIDString(dominfo['uuid'])
            except libvirt.libvirtError:
                self.assertFalse("Look up domain by uuid failed")
            if dom.isActive():
                is_active = True
                break
            timeout -= 1
            print 'sleep 1s'
            time.sleep(1)

        if not is_active:
            self.assertFalse("Domain creates failed")
        return dom

    def test_freeze_thaw_vm_rbd_disk(self):
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_freeze_thaw_vm_rbd_disk",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        dom = self._create_vm(dominfo)

        diskinfo = {"protocol": "rbd",
                    "disk_name": os.path.join(RBD_POOL, RBD_VOL1),
                    "mon_hosts": MON_HOSTS,
                    "target_dev": "vdc"
        }
        template = JJ_ENV.get_template('disk_xml.j2')
        diskxml = template.render(diskinfo)
        # 等待vm内部操作系统启动
        ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
        self.assertTrue(ssh is not None)
        ret = dom.attachDeviceFlags(diskxml, libvirt.VIR_DOMAIN_AFFECT_LIVE)
        self.assertTrue(ret >= 0)
        time.sleep(1)
        xml = dom.XMLDesc(0)
        self.assertTrue(diskinfo['disk_name'] in xml)

        try:
            ret = dom.fsFreeze(diskinfo['target_dev'])
            self.assertTrue(ret >= 0)
        except libvirt.libvirtError as exc:
            self.assertFalse(str(exc))
        try:
            ret = dom.fsInfo()
            self.assertTrue(ret >= 0)
        except libvirt.libvirtError as exc:
            self.assertTrue("guest-get-fsinfo has been disabled" in str(exc))
        except AttributeError as exc:
            print 'Warning: %s' % str(exc)
        try:
            ret = dom.fsThaw(diskinfo['target_dev'])
            self.assertTrue(ret >= 0)
        except libvirt.libvirtError as exc:
            self.assertFalse(str(exc))

        ret = dom.destroy()
        self.assertTrue(ret >= 0)

    # vm内部IO随机读写测试
    def _fio_randrw_in_vm(self, cmd, kill_nebd_server=False):
        rbd_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_fio_randrw_in_rbd_vm",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        local_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_fio_randrw_in_local_vm",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG
        }
        dominfos = [rbd_vm, local_vm]

        for dominfo in dominfos:
            dom = self._create_vm(dominfo)
            if not dom.isActive():
                self.assertFalse("Domain creates failed")

            ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
            self.assertTrue(ssh is not None)
            nebd_server_pid0 = -1
            meta_dict0 = None
            if kill_nebd_server and dominfo['disk_type'] == 'network':
                with open(os.path.join(METADATA_DIR, dominfo['uuid'])) as mf:
                    metadata = mf.read()
                meta_dict0 = json.loads(metadata)
                nebd_server_pid0 = self._get_pid_by_cmdline(dominfo['uuid'],
                                                        NEBD_SERVER_EXEC_FILE)
                self.assertTrue(nebd_server_pid0 > 0)
                # 设置定时器，5s之后杀掉nebd-server进程
                timer = Timer(5, os.kill, (nebd_server_pid0, 15))
                timer.start()

            _, out, err = ssh_exec(ssh, cmd, timeout=120)
            self.assertTrue(len(out))
            fio_ok = 0
            for line in out:
                if 'read' in line and 'IOPS' in line:
                    fio_ok += 1
                if 'write' in line and 'IOPS' in line:
                    fio_ok += 1
                if fio_ok == 2:
                    break
            self.assertEqual(2, fio_ok)

            if kill_nebd_server and dominfo['disk_type'] == 'network':
                # 检查是否重启，以及重启是否成功（pid发生变化且大于0）
                nebd_server_pid1 = self._get_pid_by_cmdline(dominfo['uuid'],
                                                        NEBD_SERVER_EXEC_FILE)
                if dominfo == rbd_vm:  # rbd镜像vm会重新拉起nebd-server
                    self.assertTrue(nebd_server_pid1 > 0)
                elif dominfo == local_vm:  # 本地镜像vm不会拉起nebd-server
                    self.assertTrue(nebd_server_pid1 < 0)
                self.assertTrue(nebd_server_pid1 != nebd_server_pid0)
                # 检查持久化文件，端口号及fd等信息不能发生变化
                with open(os.path.join(METADATA_DIR, dominfo['uuid'])) as mf:
                    metadata = mf.read()
                meta_dict1 = json.loads(metadata)
                self.assertTrue(meta_dict1 == meta_dict0)

            self._destroy_vm(dominfo)

    # vm内部fio读写测试(randrw 4k, seqrw 128k)
    def test_fio_in_vm(self):
        cmd_4k = ('/usr/bin/fio -name=/fio.data -direct=1 -iodepth=8 ' +
                  '-rw=randrw -ioengine=libaio -bs=4k -size=100M -numjobs=1')
        self._fio_randrw_in_vm(cmd_4k, kill_nebd_server=False)

        cmd_128k = ('/usr/bin/fio -name=/fio.data -direct=1 -iodepth=8 ' +
                  '-rw=rw -ioengine=libaio -bs=128k -size=200M -numjobs=1')
        self._fio_randrw_in_vm(cmd_128k, kill_nebd_server=False)

    # vm内部fio读写测试(randrw 4k, seqrw 128k)过程中kill nebd-server
    def test_fio_in_vm_kill_nebd_server(self):
        # 注意fio测试时长，要大于5s（定时器设置5s后kill nebd-server进程）
        cmd_4k = ('/usr/bin/fio -name=/fio.data -direct=1 -iodepth=8 ' +
                  '-rw=randrw -ioengine=libaio -bs=4k -size=100M ' +
                  '-numjobs=1 -rate_iops=1000')
        self._fio_randrw_in_vm(cmd_4k, kill_nebd_server=True)

        cmd_128k = ('/usr/bin/fio -name=/fio.data -direct=1 -iodepth=8 ' +
                    '-rw=rw -ioengine=libaio -bs=128k -size=200M ' +
                    '-numjobs=1 -rate_iops=60')
        self._fio_randrw_in_vm(cmd_128k, kill_nebd_server=True)

    def test_start_vm_part2_running_with_rbd_disk(self):
        # 1. 创建vm，rbd卷做系统盘
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_start_vm_part2_running_with_rbd_disk",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        dom = self._create_vm(dominfo)
        if not dom.isActive():
            self.assertFalse("Domain creates failed")
        # 等待vm内部操作系统启动（保证rpc open卷成功）
        ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
        self.assertTrue(ssh is not None)
        nebd_server_pid0 = self._get_pid_by_cmdline(dominfo['uuid'],
                                                    NEBD_SERVER_EXEC_FILE)
        self.assertTrue(nebd_server_pid0 > 0)
        metadata = None
        with open(os.path.join(METADATA_DIR, dominfo['uuid'])) as mf:
            metadata = mf.read()
        meta_dict = json.loads(metadata)
        # 检查fd大小是否在正确范围内
        for vol in meta_dict['volumes']:
            self.assertTrue(1000 >= int(vol['fd']) > 0)

        # 检查rados admin socket是否正常
        asoks = os.listdir(CEPH_CLIENT_ASOK_DIR)
        for asok in asoks:
            if asok.startswith('ceph-client.admin.%d' % nebd_server_pid0):
                asok_path = os.path.join(CEPH_CLIENT_ASOK_DIR, asok)
                cmd_asok = 'ceph daemon %s version' % asok_path
                out = run(cmd_asok)
                self.assertTrue("version" in out)

        # 2. destroy vm
        self._destroy_vm(dominfo)

        # 3. 在part2超时退出之前再次启动vm（模拟重启vm操作）
        nebd_server_pid1 = self._get_pid_by_cmdline(dominfo['uuid'],
                                                     NEBD_SERVER_EXEC_FILE)
        self.assertTrue(nebd_server_pid1 > 0)
        dom = self._create_vm(dominfo)
        if not dom.isActive():
            self.assertFalse("Domain creates failed")
        nebd_server_pid2 = self._get_pid_by_cmdline(dominfo['uuid'],
                                                     NEBD_SERVER_EXEC_FILE)
        self.assertTrue(nebd_server_pid2 > 0)
        # 等待vm内部操作系统启动（保证rpc open卷成功）
        ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
        self.assertTrue(ssh is not None)
        nebd_server_pid3 = self._get_pid_by_cmdline(dominfo['uuid'],
                                                    NEBD_SERVER_EXEC_FILE)
        self.assertTrue(nebd_server_pid3 > 0)
        self.assertTrue(nebd_server_pid0 == nebd_server_pid1
                        == nebd_server_pid2 == nebd_server_pid3)

    def test_start_vm_without_part2_with_residual_metadatafile(self):
        # 准备持久化信息文件（模拟残留）
        port = 6300
        fake_uuid = '42d57731-19f6-478f-a145-b77a6d590274'
        for uuid in [fake_uuid, UUID1, UUID2]:
            meta_file = os.path.join(METADATA_DIR, uuid)
            with open(meta_file, 'w') as mf:
                if uuid == fake_uuid:
                    mf.write(json.dumps({"port": 6200}))  # 模拟端口被占用
                else:
                    mf.write(json.dumps({"port": port}))
                    port += 1
            nebd_server_pid = self._get_pid_by_cmdline(uuid,
                                                       NEBD_SERVER_EXEC_FILE)
            self.assertTrue(nebd_server_pid < 0)
        # 1. 创建vm，rbd卷做系统盘
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_start_vm_without_part2_with_residual_metafile",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        dom = self._create_vm(dominfo)
        if not dom.isActive():
            self.assertFalse("Domain creates failed")
        # 等待vm内部操作系统启动（保证rpc open卷成功）
        ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
        self.assertTrue(ssh is not None)
        nebd_server_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                                   NEBD_SERVER_EXEC_FILE)
        self.assertTrue(nebd_server_pid > 0)
        # 检查父进程id是否为1（是否为daemon模式）
        status = []
        with open(os.path.join('/proc/', str(nebd_server_pid), 'status')) as f:
            status = f.readlines()
        for line in status:
            if line.startswith('PPid:'):
                # 0 is for docker
                self.assertTrue(int(line.split(':')[-1]) in [0, 1])
                break
        # 检查端口号是否符合预期（检查part1清理持久化文件逻辑）
        meta_file = os.path.join(METADATA_DIR, dominfo['uuid'])
        metadata = None
        with open(meta_file, 'r') as mf:
            metadata = mf.read()
        self.assertTrue(metadata)
        meta_dict = json.loads(metadata)
        self.assertTrue(int(meta_dict['port']) == 6201)
        # 2. destroy vm
        self._destroy_vm(dominfo)
        # 等待nebd server进程退出，退出后检查持久化文件是否清理
        timeout = 30
        while timeout > 0:
            timeout -= 1
            nebd_server_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                                       NEBD_SERVER_EXEC_FILE)
            if nebd_server_pid < 0:
                self.assertFalse(os.path.exists(meta_file))
                self.assertTrue(os.path.exists(
                                    os.path.join(METADATA_DIR, fake_uuid)))
                self.assertTrue(os.path.exists(
                                    os.path.join(METADATA_DIR, UUID2)))
                break
            else:
                time.sleep(1)

    def test_nebd_server_boot_args(self):
        # 创建一个使用本地文件做系统盘的虚拟机，为nebd-server心跳做准备
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_nebd_server_boot_args",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG
        }
        dom = self._create_vm(dominfo)
        if not dom.isActive():
            self.assertFalse("Domain creates failed")

        # 等待vm内部操作系统启动（保证rpc open卷成功，如果有）
        ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
        self.assertTrue(ssh is not None)
        nebd_server_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                                   NEBD_SERVER_EXEC_FILE)
        self.assertTrue(nebd_server_pid < 0)

        # 正确参数
        cmd_ok = ' '.join([NEBD_SERVER_EXEC_FILE, '-uuid', dominfo['uuid']])
        out = run(cmd_ok)
        self.assertFalse(out)
        nebd_server_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                                   NEBD_SERVER_EXEC_FILE)
        self.assertTrue(nebd_server_pid > 0)
        os.kill(nebd_server_pid, 9)
        nebd_server_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                                   NEBD_SERVER_EXEC_FILE)
        self.assertTrue(nebd_server_pid < 0)

        # 错误参数1：uuid对应的qemu进程不存在
        fake_uuid = 'e35a4a69-853b-441e-a5ec-8b8a545ce20f'
        cmd_ok = ' '.join([NEBD_SERVER_EXEC_FILE, '-uuid', fake_uuid])
        out = run(cmd_ok)
        self.assertFalse(out)
        time.sleep(1)
        nebd_server_pid = self._get_pid_by_cmdline(fake_uuid,
                                                   NEBD_SERVER_EXEC_FILE)
        self.assertTrue(nebd_server_pid < 0)
        # 错误参数2：--uuid
        cmd_ok = ' '.join([NEBD_SERVER_EXEC_FILE, '--uuid', dominfo['uuid']])
        out = run(cmd_ok)
        self.assertFalse(out)
        time.sleep(1)
        nebd_server_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                                   NEBD_SERVER_EXEC_FILE)
        self.assertTrue(nebd_server_pid < 0)

        self._destroy_vm(dominfo)

    def test_nebd_server_kill_signal(self):
        # 创建一个使用本地文件做系统盘的虚拟机，为nebd-server心跳做准备
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_nebd_server_kill_signal",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG
        }
        dom = self._create_vm(dominfo)
        if not dom.isActive():
            self.assertFalse("Domain creates failed")

        # 等待vm内部操作系统启动（保证rpc open卷成功，如果有）
        ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
        self.assertTrue(ssh is not None)
        nebd_server_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                                   NEBD_SERVER_EXEC_FILE)
        self.assertTrue(nebd_server_pid < 0)

        # SIGINT(2), SIGTERM(15)
        for sig in [2, 15]:
            port = 6300
            meta_file = os.path.join(METADATA_DIR, dominfo['uuid'])
            with open(meta_file, 'w') as mf:
                mf.write(json.dumps({"port": port}))
            cmd_ok = ' '.join([NEBD_SERVER_EXEC_FILE, '-uuid', dominfo['uuid']])
            out = run(cmd_ok)
            self.assertFalse(out)
            nebd_server_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                                       NEBD_SERVER_EXEC_FILE)
            self.assertTrue(nebd_server_pid > 0)
            # os.kill(nebd_server_pid, sig)
            # docker里执行os.kill貌似进程会收不到信号，改成kill命令就好了。。。
            cmd_kill = "/bin/kill -%d %d" % (sig, nebd_server_pid)
            out = run(cmd_kill)
            self.assertFalse(out)
            timeout = 5
            is_exit = False
            while timeout > 0:
                timeout -= 1
                nebd_server_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                                        NEBD_SERVER_EXEC_FILE)
                if nebd_server_pid < 0:
                    is_exit = True
                    break
                else:
                    time.sleep(1)
            self.assertTrue(is_exit)
            self.assertTrue(os.path.exists(meta_file))
            self._clear_all_metadatafiles()

        self._destroy_vm(dominfo)

    def _check_proc_running(self, pid, uuid, name):
        '''根据/proc/pid/cmdline文件检查进程名是否与给定的名称匹配，进程参数是否包含给定的uuid'''
        cmdline = ""
        try:
            with open(os.path.join("/proc", str(pid), "cmdline")) as f:
                cmdline = f.read()
        except IOError:
            return False

        cmdline = cmdline.split('\0')
        proc_name = os.path.basename(cmdline[0])
        if proc_name == os.path.basename(name):
            if uuid:
                if uuid in cmdline:
                    return True
                else:
                    return False
            return True
        return False

    def _get_pid_by_cmdline(self, uuid, name):
        pids = os.listdir("/proc")
        for pid in pids:
            if pid.isdigit() and self._check_proc_running(pid, uuid, name):
                return int(pid)
        return -1

    def test_force_kill_vm_qemu_proc(self):
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_force_kill_vm_qemu_proc",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        dom = self._create_vm(dominfo)
        time.sleep(5)
        qemu_pid = self._get_pid_by_cmdline(dominfo['uuid'], QEMU_PROC_NAME)
        self.assertTrue(qemu_pid > 0)
        part2_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                             NEBD_SERVER_EXEC_FILE)
        self.assertTrue(part2_pid > 0)
        os.kill(qemu_pid, 9)
        time.sleep(1)
        self.assertFalse(self._check_proc_running(qemu_pid, dominfo['uuid'],
                                                  QEMU_PROC_NAME))

        timeout = 30
        part2_exit = False
        while timeout > 0:
            if not self._check_proc_running(part2_pid, dominfo['uuid'],
                                            NEBD_SERVER_EXEC_FILE):
                part2_exit = True
                break
            timeout -= 1
            print 'sleep 1s'
            time.sleep(1)

        self.assertTrue(part2_exit)

        # TODO: 检查后端卷被close

        # 检查持久化metadata file被清理
        self.assertFalse(os.path.exists(
                            os.path.join(METADATA_DIR, dominfo['uuid'])))

    def test_create_destroy_local_vm_without_part2(self):
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_create_destroy_local_vm_without_part2",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG
        }
        dom = self._create_vm(dominfo)
        time.sleep(5)
        # 保证part2进程不会启动
        part2_pid = self._get_pid_by_cmdline(dominfo['uuid'],
                                             NEBD_SERVER_EXEC_FILE)
        self.assertTrue(part2_pid < 0)
        # 检查持久化metadata file被清理
        self.assertFalse(
            os.path.exists(os.path.join(METADATA_DIR, dominfo['uuid'])))

        # 测试destroy vm
        self._destroy_vm(dominfo)

    def test_disk_operations_in_vm(self):
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_disk_operations_in_vm",
                   "disk_type": "file",
                   "disk_file": LOCAL_IMG
        }
        dom = self._create_vm(dominfo)
        if not dom.isActive():
            self.assertFalse("Domain creates failed")

        # attach disk
        diskinfo = {"protocol": "rbd",
                    "disk_name": os.path.join(RBD_POOL, RBD_VOL1),
                    "mon_hosts": MON_HOSTS,
                    "target_dev": "vdc"
        }
        template = JJ_ENV.get_template('disk_xml.j2')
        diskxml = template.render(diskinfo)
        ret = dom.attachDeviceFlags(diskxml, libvirt.VIR_DOMAIN_AFFECT_LIVE)
        self.assertTrue(ret >= 0)
        xml = dom.XMLDesc(0)
        self.assertTrue(diskinfo['disk_name'] in xml)

        # ssh到vm内部执行如下操作：
        #       1. lsblk
        #       2. parted(mkpart)
        #       3. mkfs.(ext3, ext4, xfs)
        #           3.1 mount
        #           3.2 df
        #           3.3 dd
        #           3.4 sync
        #           3.5 umount
        #       4. parted(rm)
        ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
        self.assertTrue(ssh is not None)
        cmd_lsblk = '/usr/bin/lsblk -b|grep vd|grep disk|tail -1'
        _, out, err = ssh_exec(ssh, cmd_lsblk, timeout=3)
        self.assertTrue(len(out))
        vd = out[0].split()[0]
        self.assertTrue(vd.startswith('vd'))
        disk_size = int(out[0].split()[3])
        self.assertTrue(disk_size > (RBD_VOL1_SIZE * 0.8)
                        and disk_size < RBD_VOL1_SIZE * 1.1)
        disk = os.path.join('/dev', vd)
        cmd_parted_mkpart1 = ('/usr/sbin/parted -s %s ' % disk
                             + 'mklabel gpt mkpart primary 1 200M')
        _, out, err = ssh_exec(ssh, cmd_parted_mkpart1, timeout=5)
        self.assertTrue(out == [] and err == [])
        cmd_parted_mkpart2 = ('/usr/sbin/parted -s %s ' % disk
                             + 'mkpart primary 200M 500M')
        _, out, err = ssh_exec(ssh, cmd_parted_mkpart2, timeout=5)
        self.assertTrue(out == [] and err == [])

        for fs in ['ext3', 'ext4', 'xfs']:
            cmd_mkfs = '/sbin/mkfs.%s -F %s1' % (fs, disk)
            _, out, err = ssh_exec(ssh, cmd_mkfs, timeout=30)
            cmd_mount = '/usr/bin/mount %s1 /mnt' % disk
            _, out, err = ssh_exec(ssh, cmd_mount, timeout=5)
            cmd_df = '/bin/df | grep /mnt'
            _, out, err = ssh_exec(ssh, cmd_df, timeout=5)
            self.assertTrue(len(out) and disk in out[0])
            cmd_dd = ('/bin/dd if=/dev/zero of=/mnt/dd.zero '
                      + 'bs=1M count=100 oflag=direct')
            _, out, err = ssh_exec(ssh, cmd_dd, timeout=30)
            _, out, err = ssh_exec(ssh, 'sync', timeout=30)
            cmd_umount = '/usr/bin/umount /mnt'
            _, out, err = ssh_exec(ssh, cmd_umount, timeout=5)

        cmd_parted_rmpart1 = '/usr/sbin/parted -s %s ' % disk + 'rm 1'
        _, out, err = ssh_exec(ssh, cmd_parted_rmpart1, timeout=5)
        self.assertTrue(out == [] and err == [])
        cmd_parted_rmpart2 = '/usr/sbin/parted -s %s ' % disk + 'rm 2'
        _, out, err = ssh_exec(ssh, cmd_parted_rmpart2, timeout=5)
        self.assertTrue(out == [] and err == [])
        cmd_lsblk = '/usr/bin/lsblk -l|grep %s|grep part' % vd
        _, out, err = ssh_exec(ssh, cmd_lsblk, timeout=3)
        self.assertTrue(out == [])

        # 卸载卷
        ret = dom.detachDeviceFlags(diskxml, libvirt.VIR_DOMAIN_AFFECT_LIVE)
        self.assertTrue(ret >= 0)
        xml = dom.XMLDesc(0)
        self.assertTrue(diskinfo['disk_name'] not in xml)

        # 挂载scsi disk，测试fstrim操作
        diskinfo = {"protocol": "rbd",
                    "disk_name": os.path.join(RBD_POOL, RBD_VOL1),
                    "mon_hosts": MON_HOSTS,
                    "target_dev": "sdc"
        }
        template = JJ_ENV.get_template('disk_xml.j2')
        diskxml = template.render(diskinfo)
        ret = dom.attachDeviceFlags(diskxml, libvirt.VIR_DOMAIN_AFFECT_LIVE)
        self.assertTrue(ret >= 0)
        xml = dom.XMLDesc(0)
        self.assertTrue(diskinfo['disk_name'] in xml and 'scsi' in xml)
        ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
        self.assertTrue(ssh is not None)
        cmd_lsblk = '/usr/bin/lsblk -b|grep sd|grep disk|tail -1'
        _, out, err = ssh_exec(ssh, cmd_lsblk, timeout=3)
        self.assertTrue(len(out))
        sd = out[0].split()[0]
        self.assertTrue(sd.startswith('sd'))
        disk = os.path.join('/dev', sd)
        cmd_mkfs = '/sbin/mkfs.ext4 -F %s' % disk
        _, out, err = ssh_exec(ssh, cmd_mkfs, timeout=30)
        cmd_mount = '/usr/bin/mount %s /mnt' % disk
        _, out, err = ssh_exec(ssh, cmd_mount, timeout=5)
        cmd_df = '/bin/df | grep /mnt'
        _, out, err = ssh_exec(ssh, cmd_df, timeout=5)
        self.assertTrue(len(out) and disk in out[0])
        cmd_dd = ('/bin/dd if=/dev/zero of=/mnt/dd.zero '
                  + 'bs=1M count=100 oflag=direct')
        _, out, err = ssh_exec(ssh, cmd_dd, timeout=30)
        cmd_rm = '/bin/rm /mnt/dd.zero'
        _, out, err = ssh_exec(ssh, cmd_rm, timeout=5)
        cmd_trim = '/sbin/fstrim /mnt'
        _, out, err = ssh_exec(ssh, cmd_trim, timeout=30)
        self.assertTrue(out == [] and err == [])

        self._destroy_vm(dominfo)

    def test_resize_rbd_disk(self):
        dominfo = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_resize_rbd_disk",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        dom = self._create_vm(dominfo)
        time.sleep(5)

        if dom.isActive():
            capacity = dom.blockInfo('vda')[0]  # Byte
            new_size = (capacity + RBD_VM_IMG_SIZE) / 1024  # KB
            ret = dom.blockResize('vda', new_size)
            self.assertTrue(ret >= 0)
            self.assertTrue(dom.blockInfo('vda')[0] >
                            (capacity + RBD_VM_IMG_SIZE * 0.8))
        else:
            self.assertFalse("Domain creates failed")
        self._destroy_vm(dominfo)

    def test_live_upgrade_ceph_client(self):
        old_version = CEPH_CLIENT_REAL_VERSION[CEPH_CLIENT_OLD_PACKAGE]
        new_version = CEPH_CLIENT_REAL_VERSION[CEPH_CLIENT_NEW_PACKAGE]
        packages = ['ceph', 'ceph-common', 'librbd1', 'python-rados',
                    'python-cephfs', 'python-rbd', 'librados2',
                    'libcephfs1', 'python-ceph']

        rbd_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                   "name": "test_live_upgrade_ceph_client",
                   "disk_type": "network",
                   "protocol": "rbd",
                   "disk_name": os.path.join(RBD_POOL, RBD_VM_IMG),
                   "mon_hosts": MON_HOSTS
        }
        local_vm = {"uuid": UUID1, "cpuset": '0-%d' % (MAX_CPUS - 1),
                    "name": "test_live_upgrade_ceph_client",
                    "disk_type": "file",
                    "disk_file": LOCAL_IMG
        }
        dominfos = [rbd_vm, local_vm]

        for dominfo in dominfos:
            # 安装ceph client旧包
            cmd_install_old_package = 'apt-get install'
            for p in packages:
                cmd_install_old_package = ' '.join([cmd_install_old_package, p])
                cmd_install_old_package += ('=%s' % CEPH_CLIENT_OLD_PACKAGE)
            cmd_install_old_package += ' -y --force-yes'
            run(cmd_install_old_package, timeout=30)
            out = run('/usr/bin/dpkg -l librbd1')

            self.assertTrue(out and CEPH_CLIENT_OLD_PACKAGE in out)
            dom = self._create_vm(dominfo)
            if not dom.isActive():
                self.assertFalse("Domain creates failed")
            # attach disk
            diskinfo = {"protocol": "rbd",
                        "disk_name": os.path.join(RBD_POOL, RBD_VOL1),
                        "mon_hosts": MON_HOSTS,
                        "target_dev": "vdc"
            }
            template = JJ_ENV.get_template('disk_xml.j2')
            diskxml = template.render(diskinfo)
            ret = dom.attachDeviceFlags(diskxml, libvirt.VIR_DOMAIN_AFFECT_LIVE)
            self.assertTrue(ret >= 0)
            xml = dom.XMLDesc(0)
            self.assertTrue(diskinfo['disk_name'] in xml)

            ssh = create_ssh_connect(VM_HOST_IP, timeout=120)
            self.assertTrue(ssh is not None)
            cmd_lsblk = '/usr/bin/lsblk -b|grep vd|grep disk|tail -1'
            _, out, err = ssh_exec(ssh, cmd_lsblk, timeout=3)
            self.assertTrue(len(out))
            vd = out[0].split()[0]
            self.assertTrue(vd.startswith('vd'))
            disk = os.path.join('/dev', vd)
            cmd_dd = ('/bin/dd if=/dev/zero of=%s ' % disk
                      + 'bs=1M count=100 oflag=direct')
            _, out, err = ssh_exec(ssh, cmd_dd, timeout=30)
            self.assertEqual([], out)
            self.assertTrue(len(err) and 'records' in err[0])

            # 安装ceph client新包
            cmd_install_new_package = 'apt-get install'
            for p in packages:
                cmd_install_new_package = ' '.join([cmd_install_new_package, p])
                cmd_install_new_package += ('=%s' % CEPH_CLIENT_NEW_PACKAGE)
            cmd_install_new_package += ' -y --force-yes'
            run(cmd_install_new_package, timeout=30)
            out = run('/usr/bin/dpkg -l librbd1')
            self.assertTrue(out and CEPH_CLIENT_NEW_PACKAGE in out)

            nebd_server_pid0 = self._get_pid_by_cmdline(dominfo['uuid'],
                                                        NEBD_SERVER_EXEC_FILE)
            self.assertTrue(nebd_server_pid0 > 0)
            # 检查rados admin socket是否正常
            asoks = os.listdir(CEPH_CLIENT_ASOK_DIR)
            for asok in asoks:
                if asok.startswith('ceph-client.admin.%d' % nebd_server_pid0):
                    asok_path = os.path.join(CEPH_CLIENT_ASOK_DIR, asok)
                    cmd_asok = 'ceph daemon %s version' % asok_path
                    out = run(cmd_asok)
                    self.assertTrue(out and "version" in out and
                                    old_version in out)
            os.kill(nebd_server_pid0, 15)
            timeout = 10
            is_reboot = False
            while timeout > 0:
                timeout -= 1
                nebd_server_pid1 = self._get_pid_by_cmdline(dominfo['uuid'],
                                                        NEBD_SERVER_EXEC_FILE)
                if (nebd_server_pid1 > 0 and
                        nebd_server_pid1 != nebd_server_pid0):
                    is_reboot = True
                    asoks = os.listdir(CEPH_CLIENT_ASOK_DIR)
                    for asok in asoks:
                        if asok.startswith('ceph-client.admin.%d' %
                                           nebd_server_pid1):
                            asok_path = os.path.join(CEPH_CLIENT_ASOK_DIR, asok)
                            cmd_asok = 'ceph daemon %s version' % asok_path
                            out = run(cmd_asok)
                            self.assertTrue(out and "version" in out and
                                            new_version in out)
                    break
                else:
                    time.sleep(1)
            self.assertTrue(is_reboot)
            _, out, err = ssh_exec(ssh, cmd_dd, timeout=30)
            self.assertEqual([], out)
            self.assertTrue(len(err) and 'records' in err[0])
            self._destroy_vm(dominfo)
            self._killall_nebd_servers()

    def test_live_upgrade_curve_client(self):
        # TODO
        pass

    def test_ceph_rbd_rados_cli(self):
        try:
            ceph_cmd_base = ('ceph -c %s ' % CEPH_CONF
                             + '--admin_socket /tmp/ceph-client.asok ')
            out = run(ceph_cmd_base + '-s')
            self.assertTrue('cluster' in out)
            out = run(ceph_cmd_base + 'osd tree')
            self.assertTrue('ID' in out and 'WEIGHT' in out)
            out = run(ceph_cmd_base + 'osd pool ls detail')
            self.assertTrue('pool' in out and RBD_POOL in out)

            rbd_cmd_base = ('rbd -c %s ' % CEPH_CONF
                             + '--admin_socket /tmp/ceph-client.asok ')
            out = run(rbd_cmd_base + 'ls -p %s' % RBD_POOL)
            self.assertTrue(RBD_VM_IMG in out)
            out = run(rbd_cmd_base
                      + 'info %s' % os.path.join(RBD_POOL, RBD_VM_IMG))
            self.assertTrue(RBD_VM_IMG in out)
            out = run(rbd_cmd_base
                      + 'create --image-format 2 --size 128 %s'
                        % os.path.join(RBD_POOL, 'rbd-cli-test'))
            self.assertFalse(out)
            out = run(rbd_cmd_base
                      + 'snap create %s'
                        % os.path.join(RBD_POOL, 'rbd-cli-test@snap-test'))
            self.assertFalse(out)
            out = run(rbd_cmd_base
                      + 'snap ls %s' % os.path.join(RBD_POOL, 'rbd-cli-test'))
            self.assertTrue('snap-test' in out)
            out = run(rbd_cmd_base
                      + 'snap rm %s'
                        % os.path.join(RBD_POOL, 'rbd-cli-test@snap-test'))
            self.assertFalse(out)
            out = run(rbd_cmd_base
                      + 'rm %s' % os.path.join(RBD_POOL, 'rbd-cli-test'),
                      ignore='Removing image')
            self.assertFalse(out)

            rados_cmd_base = ('rados -c %s ' % CEPH_CONF
                             + '--admin_socket /tmp/ceph-client.asok ')
            out = run(rados_cmd_base + 'lspools')
            self.assertTrue(RBD_POOL in out)
            out = run(rados_cmd_base + 'df')
            self.assertTrue(RBD_POOL in out)
        except Exception as exc:
            self.assertFalse(str(exc))


def _get_rbd_ctx():
    cluster = rados.Rados(conffile=CEPH_CONF)
    cluster.connect()
    ioctx = cluster.open_ioctx(RBD_POOL)
    rbd_inst = rbd.RBD()
    return (ioctx, rbd_inst, cluster)


def prepare_rbd_disk():
    if REUSE_RBD_DISK:
        return
    clear_rbd_disk()

    print 'going to create all rbd disks...'
    ioctx, rbd_inst, cluster = _get_rbd_ctx()
    rbd_inst.create(ioctx, RBD_VOL1, RBD_VOL1_SIZE,
                    22, old_format=False, features=1)
    rbd_inst.create(ioctx, RBD_VM_IMG, RBD_VM_IMG_SIZE,
                    22, old_format=False, features=1)
    image = rbd.Image(ioctx, RBD_VM_IMG)
    # LOCAL_IMG需要为raw格式镜像，虚拟机xml里面指定了raw
    # 并且导入到rbd卷也需要是raw格式
    f = open(LOCAL_IMG, "rb")
    chunk_size = 4*1024*1024
    try:
        offset = 0
        data = f.read(chunk_size)
        while data != "":
            print "Writing data at offset " + str(offset) + \
                  "(" + str(offset / 1024 / 1024) + "MB)"
            offset += image.write(data, offset)
            data = f.read(chunk_size)
    finally:
        f.close()
        image.close()
        ioctx.close()
        cluster.shutdown()


def clear_all_vms():
    print 'going to clear all vms...'
    doms = LIBVIRT_CONN.listAllDomains()
    for dom in doms:
        if dom.isActive():
            dom.destroy()
    doms = LIBVIRT_CONN.listAllDomains()
    for dom in doms:
        if dom.isPersistent():
            dom.undefine()


def clear_rbd_disk():
    if REUSE_RBD_DISK:
        return
    print 'going to rm all rbd disks...'
    ioctx, rbd_inst, cluster = _get_rbd_ctx()
    for img in [RBD_VM_IMG, RBD_VOL1]:
        try:
            rbd_inst.remove(ioctx, img)
        except rbd.ImageNotFound:
            pass
        except Exception as exc:
            ioctx.close()
            cluster.shutdown()
            raise exc
    ioctx.close()
    cluster.shutdown()


def clear_log_dirs():
    print 'going to clear all log files...'
    for d in ['/var/log/nebd', '/var/log/libvirt/qemu']:
        fs = os.listdir(d)
        for f in fs:
            os.remove(os.path.join(d, f))


def install_rbd_client():
    # 安装ceph client新包
    packages = ['ceph', 'ceph-common', 'librbd1', 'python-rados',
                    'python-cephfs', 'python-rbd', 'librados2',
                    'libcephfs1', 'python-ceph']
    cmd_install_new_package = 'apt-get install'
    for p in packages:
        cmd_install_new_package = ' '.join([cmd_install_new_package, p])
        cmd_install_new_package += ('=%s' % CEPH_CLIENT_NEW_PACKAGE)
    cmd_install_new_package += ' -y --force-yes'
    run(cmd_install_new_package, timeout=30)
    out = run('/usr/bin/dpkg -l grep librbd1')
    assert(out and CEPH_CLIENT_NEW_PACKAGE in out)


if __name__ == '__main__':
    if LIBVIRT_CONN is None:
        LIBVIRT_CONN = libvirt.open(None)
    clear_all_vms()
    clear_log_dirs()
    install_rbd_client()
    prepare_rbd_disk()
    path = '{}/templates/'.format(os.path.dirname(os.path.abspath(__file__)))
    if JJ_ENV is None:
        JJ_ENV = Environment(loader=FileSystemLoader(path))

    unittest.main()

    LIBVIRT_CONN.close()
    clear_rbd_disk()

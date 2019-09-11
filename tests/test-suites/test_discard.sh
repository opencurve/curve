#!/bin/bash

POOL=rbd
MON_HOST=10.182.30.27

# 从snap创建系统盘
rbd clone $POOL/image-centos_7.5@snap $POOL/vm-discard
rbd create --image-format 2 --size 8388608 $POOL/disk-8T

# 从系统盘创建1个vm
echo '''
<domain type="kvm">
  <name>vm-discard</name>
  <memory>2097152</memory>
  <vcpu>2</vcpu>
  <os>
    <type>hvm</type>
    <boot dev="hd"/>
  </os>
  <features>
    <acpi/>
    <apic/>
  </features>
  <cpu mode="host-model" match="exact"/>
  <devices>
    <emulator>/usr/bin/qemu-system-x86_64</emulator>
    <disk type="network" device="disk">
      <driver type="raw" cache="none" discard="unmap"/>
      <source protocol="rbd" name="#POOL#/vm-discard">
          <host name="#MON_HOST#" port="6789"/>
      </source>
      <target bus="scsi" dev="sda"/>
    </disk>
    <disk type="network" device="disk">
      <driver type="raw" cache="none" discard="unmap"/>
      <source protocol="rbd" name="#POOL#/disk-8T">
          <host name="#MON_HOST#" port="6789"/>
      </source>
      <target bus="scsi" dev="sdb"/>
    </disk>
    <controller type="scsi" index="0" model="virtio-scsi">
      <address type="pci" domain="0x0000" bus="0x00" slot="0x0b" function="0x0"/>
    </controller>
    <interface type="bridge">
      <model type="virtio"/>
      <source bridge="bri"/>
    </interface>
    <input type="tablet" bus="usb"/>
    <graphics type="vnc" autoport="yes" keymap="en-us" listen="0.0.0.0"/>
    <channel type="unix">
      <source mode="bind" path="/var/lib/libvirt/qemu/org.qemu.guest_agent.0.vm-discard.sock"/>
      <target type="virtio" name="org.qemu.guest_agent.0"/>
    </channel>
  </devices>
</domain>
''' > vm-discard.xml
sed -i "s/#POOL#/$POOL/g" vm-discard.xml
sed -i "s/#MON_HOST#/$MON_HOST/g" vm-discard.xml
sudo virsh define vm-discard.xml
sudo virsh start vm-discard

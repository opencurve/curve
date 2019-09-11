#!/bin/bash

# 创建10个卷
POOL=rbd
MON_HOST=10.182.30.27

for i in {1..10}; do
    rbd create -p $POOL vol-$i --image-format 2 --size 1024
done


rbd snap create $POOL/rbd_sys_disk@snap
rbd snap protect $POOL/rbd_sys_disk@snap

# 创建1个vm
rbd clone $POOL/rbd_sys_disk@snap $POOL/vm-with-10-disk

echo '''
<domain type="kvm">
  <name>vm-with-10-disk</name>
  <memory>8388608</memory>
  <vcpu>4</vcpu>
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
      <driver type="raw" cache="none"/>
      <source protocol="rbd" name="#POOL#/vm-with-10-disk">
          <host name="#MON_HOST#" port="6789"/>
      </source>
      <target bus="virtio" dev="vda"/>
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
      <source mode="bind" path="/var/lib/libvirt/qemu/org.qemu.guest_agent.0.vm-with-10-disk.sock"/>
      <target type="virtio" name="org.qemu.guest_agent.0"/>
    </channel>
  </devices>
</domain>
''' > vm-with-10-disk.xml
sed -i "s/#POOL#/$POOL/g" vm-with-10-disk.xml
sed -i "s/#MON_HOST#/$MON_HOST/g" vm-with-10-disk.xml

sudo virsh define vm-with-10-disk.xml
sudo virsh start vm-with-10-disk
sleep 10

# 挂载10个卷到vm
target_devs=(vdc vdd vde vdf vdg vdh vdi vdj vdk vdl)
for i in {0..9}; do
echo '''<disk type="network" device="disk">
    <driver type="raw" cache="none"/>
    <target bus="virtio" dev="#target_dev#"/>
    <source protocol="rbd" name="#disk_name#">
        <host name="#MON_HOST#" port="6789"/>
    </source>
</disk>
''' > ./disk.xml
sed -i "s/#target_dev#/${target_devs[$i]}/" ./disk.xml
sed -i "s/#disk_name#/$POOL\/vol-$((i+1))/" ./disk.xml
sed -i "s/#MON_HOST#/$MON_HOST/g" ./disk.xml
sudo virsh attach-device vm-with-10-disk disk.xml
sleep 1
done

# vm里面对挂载的卷跑fio

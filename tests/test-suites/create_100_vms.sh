#!/bin/bash

POOL=rbd
MON_HOST=10.182.30.27

for i in {1..90}; do
echo ====$i====
# 从snap创建系统盘
rbd rm $POOL/vm-$i
rbd clone $POOL/rbd_sys_disk@snap $POOL/vm-$i

# 从系统盘创建1个vm
echo '''
<domain type="kvm">
  <name>#vm-#</name>
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
      <driver type="raw" cache="none"/>
      <source protocol="rbd" name="#POOL#/#vm-#">
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
      <source mode="bind" path="/var/lib/libvirt/qemu/org.qemu.guest_agent.0.#vm-#.sock"/>
      <target type="virtio" name="org.qemu.guest_agent.0"/>
    </channel>
  </devices>
</domain>
''' > vm-$i.xml

sed -i "s/#POOL#/$POOL/g" vm-$i.xml
sed -i "s/#MON_HOST#/$MON_HOST/g" vm-$i.xml
sed -i "s/#vm-#/vm-$i/g" vm-$i.xml

sudo virsh undefine vm-$i
sudo virsh define vm-$i.xml
sudo virsh start vm-$i
sleep 1

done

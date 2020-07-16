### 一、项目说明

该项目用于通过NBD(Network Block Device)技术将curve文件映射到本地，直接提供本地裸设备的形式进行使用。

### 二、使用方法

1.安装curve-sdk包，如果是通过dpkg命令安装，则需要手动安装 libnl-3-dev libnl-genl-3-dev 这两个package

2.安装nebd包，nebd使用方法(TODO 后续开源版本请添加相应文档)

3.通过sudo nebd-daemon.sh start启动nebd server

4.完成上面安装后，可通过下面介绍的命令将文件作为网络块设备挂载到本地

### 三、命令介绍

```
Usage: curve-nbd [options] map <image>  Map an image to nbd device
                 unmap <device|image>   Unmap nbd device
                 [options] list-mapped  List mapped nbd devices
Map options:
  --device <device path>  Specify nbd device path (/dev/nbd{num})
  --read-only             Map read-only
  --nbds_max <limit>      Override for module param nbds_max
  --max_part <limit>      Override for module param max_part
  --timeout <seconds>     Set nbd request timeout
  --try-netlink           Use the nbd netlink interface
```

**命令说明**

map：将curve文件映射到本地，映射成功后，可以通过lsblk看到nbd设备

unmap：将映射的nbd设备卸载掉，并关闭nbd进程

list-mapped：列举已经映射的文件和nbd设备的对应关系

**选项说明**

--device path  指定文件的挂载路径，例如/dev/nbd0；如果不指定，会自动选择一个可用的路径挂载

--read-only  指定该参数挂载出来的块设备将不允许写入

--nbds_max limit  载入 NBD 内核模块时覆盖其参数，用于限制 nbd 设备的数量

--max_part limit  指定块设备允许的最大分区数量

nbds_max和max_part参数只有在nbd模块驱动没有加载情况下才会生效；如果已经加载，将无法生效

--timeout sec  指定请求的超时时间，默认为1小时；如果请求超时，块设备自动退出

--try-netlink  是否使用netlink的方式与nbd内核通信；如果系统不支持netlink，将自动采用ioctl方式

**映像名规则**

后端如果要使用热升级，则指定image-spec格式为"**cbd:poolname/filename_username_:** "例如： cbd:pool1//cinder/volume-6f30d296-07f7-452e-a983-513191f8cd95_cinder_:

### 四、目录树说明

.
├── BUILD                                   #BAZEL BUILD文件
├── ImageInstance.cpp                       #封装后端文件操作接口实现，基于libnebd实现
├── ImageInstance.h                         #封装后端文件操作接口
├── NBDController.cpp                       #控制NBD内核相关操作接口实现
├── NBDController.h                         #控制NBD内核相关操作接口，包含IOCtl方式的接口和NetLink方式的接口
├── NBDServer.cpp                           #NBD Server相关接口实现
├── NBDServer.h                             #NBD Server相关接口，运行在用户态，通过socketpair和NBD内核态通信
├── NBDTool.cpp                             #NBD管理接口实现
├── NBDTool.h                               #NBD管理接口，包括map、unmap、list
├── NBDWatchContext.cpp	                    #Curve文件监控接口实现
├── NBDWatchContext.h                       #Curve文件监控接口，用于Curve文件状态变化时，更新对应的NBD设备属性，例如文件大小
├── README.md                               #项目说明
├── SafeIO.cpp                              #安全读写接口实现
├── SafeIO.h                                #安全读写接口，用于保证对文件或socket等读写时，出现异常情况时，进行特别处理
├── argparse.cpp                            #参数解析相关接口实现
├── argparse.h                              #参数解析相关接口，解析curve-nbd命令参数
├── define.h                                #项目内的全局定义
├── interruptible_sleeper.h                 #可中断的sleeper接口实现
├── libnebd.h                               #libnebd接口
├── main.cpp                                #主函数实现
├── nbd-netlink.h                           #nbd-netlink接口声明
├── texttable.cpp                           #格式化list结果输出相关接口实现
├── texttable.h                             #格式化list结果输出相关接口
├── util.cpp                                #公共辅助接口实现
└── util.h                                  #公共辅助接口

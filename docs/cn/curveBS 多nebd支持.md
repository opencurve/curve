# 多NEBD支持

## 1 目的

在同一台物理机上，可以为不同用户启动相应的nebd-server进程。从而可以配合路由规则，限制不同用户的流量走不同的网卡。

## 2 使用方式
目前仅支持curve-nbd方式，云主机需要修改相应的代码并升级，暂时未支持。

### 2.1 环境配置
需要启动nebd-server的用户，需要修改三个配置文件。

**/etc/nebd/users**

记录需要启动nebd-server的用户。

例如：

```
root  ; 加不加都行，为了保持兼容性，start/stop/restart/status命令默认会为root用户操作
nbs
spark
```

**/etc/nebd/nebd-server-${USER}.conf**

相关配置项

```
curveclient.confPath：curve client配置文件路径，需要确保对应文件存在。(可以不修改）

listen.address：nebd-sever监听的unix domain socket，用来接受nebd-client的请求。修改格式为：/data/nebd/nebd-${USER}/nebd.sock

meta.file.path：nebd-server持久化元数文件。修改格式为：/data/nebd/nebd-${USER}/nebdserver.meta
```

**/etc/nebd/nebd-client-${USER}.conf**

相关配置项
```
nebdserver.serverAddress：nebd-server监听的unix domain socket，需要与nebd-server配置文件中的listen.address一致

metacache.fileLockPath：文件锁目录，nebd-client会为挂载的卷创建文件锁，放在该目录下。修改格式为：/data/nebd/nebd-${USER}/lock

log.path：日志文件路径。修改格式为：/data/log/nebd/nebd-${USER}/client
```

**注:**

1. ${USER}为对应的用户名。
2. 为了保持兼容性，root用户的配置文件是/etc/nebd/nebd-server.conf和/etc/nebd/nebd-client.conf，其中的配置项一般不用修改。

### 2.2 操作命令

**start/stop/restart/status**

为/etc/nebd/users中记录的所有用户，统一执行对应的命令。这里为了保持兼容性，即便/etc/nebd/users中未记录root用户，也会对root用户的nebd-server进程进行操作。

执行命令时，需要以root或加上sudo执行。

**start_one/stop_one/restart_one/status_one**

只为当前执行命令的用户执行对应的操作。

### 2.3 curve-nbd挂载卷
由于curve-nbd挂载卷时需要访问/dev/nbdX设备，只能以root用户或sudo方式运行。所以如果想要使curve-nbd map连向不同的nebd-server，可以在挂载卷时，指定--nebd-conf参数。

```
curve-nbd map cbd:pool//filename_user_ --nebd-conf /etc/nebd/nebd-client-nbs.conf    # 连向nbs用户启动的nebd-server
curve-nbd map cbd:pool//filename_user_    # 未指定的情况下，默认连向root用户启动的nebd-server
```

同时，参数会持久化到 **/etc/curve/curvetab** 文件，用于开机自启动。

# localsnapshotclone tools api

本地快照和克隆采用tools-v2工具命令行方式，使用方法如下：

## 创建快照：

##### 描述

创建一个快照。

##### 使用方法

```
Usage:  curve bs create snapshot [flags]

create snapshot for file in curvebs cluster

Flags:
  -f, --format string         output format (json|plain) (default "plain")
      --mdsaddr string        mds address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702
      --password string       user password (default "root_password")
      --rpcretrytimes int32   rpc retry times (default 1)
      --rpctimeout duration   rpc timeout (default 10s)
      --snappath string       snap file path, like: {file-path}@{snap-name}[required]
      --user string           user name[required]

Global Flags:
  -c, --conf string   config file (default is $HOME/.curve/curve.yaml or /etc/curve/curve.yaml)
  -h, --help          print help
      --showerror     display all errors in command
      --verbose       show some log

Examples:
$ curve bs create snapshot --snappath /test0@snap0 --user curve
```

## 删除快照

##### 描述

删除一个快照。

##### 使用方法

```
Usage:  curve bs delete snapshot [flags]

delete snapshot for file in curvebs cluster

Flags:
  -f, --format string         output format (json|plain) (default "plain")
      --mdsaddr string        mds address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702
      --password string       user password (default "root_password")
      --rpcretrytimes int32   rpc retry times (default 1)
      --rpctimeout duration   rpc timeout (default 10s)
      --snappath string       snap file path, like: {file-path}@{snap-name}[required]
      --user string           user name[required]

Global Flags:
  -c, --conf string   config file (default is $HOME/.curve/curve.yaml or /etc/curve/curve.yaml)
  -h, --help          print help
      --showerror     display all errors in command
      --verbose       show some log

Examples:
$ curve bs delete snapshot --snappath /test0@snap0 --user curve
```

## 列出快照

##### 描述

列出所有快照。

##### 使用方法

```
Usage:  curve bs list snapshot [flags]

list snapshot information for volume in curvebs

Flags:
  -f, --format string         output format (json|plain) (default "plain")
      --mdsaddr string        mds address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702
      --password string       user password (default "root_password")
      --path string           file path[required]
      --rpcretrytimes int32   rpc retry times (default 1)
      --rpctimeout duration   rpc timeout (default 10s)
      --user string           user name[required]

Global Flags:
  -c, --conf string   config file (default is $HOME/.curve/curve.yaml or /etc/curve/curve.yaml)
  -h, --help          print help
      --showerror     display all errors in command
      --verbose       show some log

Examples:
$ curve bs list snapshot --path /test0 --user curve
```

## 保护快照

##### 描述

保护一个快照, 用于克隆

#### 使用方法

```
Usage:  curve bs protect [flags]

protect snapshot in curvebs cluster

Flags:
  -f, --format string         output format (json|plain) (default "plain")
      --mdsaddr string        mds address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702
      --password string       user password (default "root_password")
      --rpcretrytimes int32   rpc retry times (default 1)
      --rpctimeout duration   rpc timeout (default 10s)
      --snappath string       snap file path, like: {file-path}@{snap-name}[required]
      --user string           user name[required]

Global Flags:
  -c, --conf string   config file (default is $HOME/.curve/curve.yaml or /etc/curve/curve.yaml)
  -h, --help          print help
      --showerror     display all errors in command
      --verbose       show some log

Examples:
$ curve bs protect --snappath /test0@snap0 --user curve
```

## 取消保护快照

##### 描述

取消保护一个快照

#### 使用方法

```
Usage:  curve bs unprotect [flags]

unprotect snapshot in curvebs cluster

Flags:
  -f, --format string         output format (json|plain) (default "plain")
      --mdsaddr string        mds address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702
      --password string       user password (default "root_password")
      --rpcretrytimes int32   rpc retry times (default 1)
      --rpctimeout duration   rpc timeout (default 10s)
      --snappath string       snap file path, like: {file-path}@{snap-name}[required]
      --user string           user name[required]

Global Flags:
  -c, --conf string   config file (default is $HOME/.curve/curve.yaml or /etc/curve/curve.yaml)
  -h, --help          print help
      --showerror     display all errors in command
      --verbose       show some log

Examples:
$ curve bs unprotect --snappath /test0@snap0 --user curve
```

## children

##### 描述

列出卷或者快照的所有子卷

#### 使用方法

```
Usage:  curve bs children [flags]

list children of snapshot/volume in curvebs cluster

Flags:
  -f, --format string         output format (json|plain) (default "plain")
      --mdsaddr string        mds address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702
      --password string       user password (default "root_password")
      --path string           file or directory path (default "/test")
      --rpcretrytimes int32   rpc retry times (default 1)
      --rpctimeout duration   rpc timeout (default 10s)
      --snappath string       snapshot file path
      --user string           user name[required]

Global Flags:
  -c, --conf string   config file (default is $HOME/.curve/curve.yaml or /etc/curve/curve.yaml)
  -h, --help          print help
      --showerror     display all errors in command
      --verbose       show some log

Examples:
$ curve bs children --path /test0 --user curve
$ curve bs children --snappath /test0@snap0 --user curve
```

## 克隆：

##### 描述

从快照克隆一个子卷

#### 使用方法

```
Usage:  curve bs clone [flags]

clone file in curvebs cluster

Flags:
      --dstpath string        destiation file path[required]
  -f, --format string         output format (json|plain) (default "plain")
      --mdsaddr string        mds address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702
      --password string       user password (default "root_password")
      --rpcretrytimes int32   rpc retry times (default 1)
      --rpctimeout duration   rpc timeout (default 10s)
      --snappath string       snap file path, like: {file-path}@{snap-name}[required]
      --user string           user name[required]

Global Flags:
  -c, --conf string   config file (default is $HOME/.curve/curve.yaml or /etc/curve/curve.yaml)
  -h, --help          print help
      --showerror     display all errors in command
      --verbose       show some log

Examples:
$ curve bs clone --snappath /test0@snap0 --dstpath /test2 --user curve
```

## Flatten

##### 描述

补足子卷的数据，使其与父卷和快照解耦

#### 使用方法

```
Usage:  curve bs flatten [flags]

flatten clone file in curvebs cluster

Flags:
  -f, --format string         output format (json|plain) (default "plain")
      --mdsaddr string        mds address, should be like 127.0.0.1:6700,127.0.0.1:6701,127.0.0.1:6702
      --password string       user password (default "root_password")
      --path string           file path[required]
      --rpcretrytimes int32   rpc retry times (default 1)
      --rpctimeout duration   rpc timeout (default 10s)
      --user string           user name[required]

Global Flags:
  -c, --conf string   config file (default is $HOME/.curve/curve.yaml or /etc/curve/curve.yaml)
  -h, --help          print help
      --showerror     display all errors in command
      --verbose       show some log

Examples:
$ curve bs flatten --path /test2 --user curve
```




# curve tool 开发者指南

curve 工具是 Curve 团队为了提高系统的易用性，解决旧工具种类多输出繁琐等问题而设计的工具，
主要用于对 Curve 块存储集群和 Curve 文件存储集群进行运维的工具。


## 目录

- [curve tool 开发者指南](#curve-tool-开发者指南)
  - [目录](#目录)
  - [整体设计](#整体设计)
  - [项目组织结构](#项目组织结构)
  - [Curve 命令的实现（添加）](#curve-命令的实现添加)
  - [Curve 命令开发调试](#curve-命令开发调试)
    - [部署 Curve 集群](#部署-curve-集群)
    - [环境准备](#环境准备)
    - [编译](#编译)
    - [调试](#调试)
    - [调试流程](#调试流程)

## 整体设计

用户可以通过以下文档来了解 Curve 工具的整体设计：

> [curve 工具](../../.././docs/cn/curve工具.md)

## 项目组织结构

Curve 项目组织结构参照的是 [project-layout](https://github.com/golang-standards/project-layout/blob/master/README_zh.md)，各目录详情如下：

```shell
├── cmd                 # 项目主干
├── docs                # 项目相关的文档
├── internal            # 私有库代码
│   ├── error           # 错误码
│   ├── proto           # 工具内部定义的 protobuf （主要是为了块存储的错误输出）
│   └── utils           # utils
├── Makefile            # 编译 curve
├── mk-proto.sh         # 生成 protobuf 脚本
├── pkg                 # 公共代码库
│   ├── cli             # 命令相关
│   │   └── command
│   │       ├── curvebs # curvebs 命令
│   │       ├── curvefs # curvefs 命令
│   │       └── version # curve 命令版本
│   ├── config          # curve 读取配置
│   └── output          # curve 输出
└── proto               # curve 工程 protobuf 生成相关代码
    ├── curvefs         # curvefs protobuf 生成代码
    └── proto           # curvebs protobuf 生成代码

```

## Curve 命令的实现（添加）

curve 的命令分为两大类：

1. 中间命令
   不负责命令的具体的执行，是最终命令的上层命令。
2. 最终命令
   命令的具体执行。分为单 rpc 命令 和多条最终命令聚合。
3. 根命令
   根命令是一种特殊的中间命令，即为 curve。

以命令 curve bs list server 为例：
curve bs list 为中间命令，server 为最终命令。其中 bs list 对应的 go 文件分别为：[bs.go](../../pkg/cli/command/curvebs/bs.go) 和 [list.go](../../pkg/cli/command/curvebs/list/list.go)；server 对应的 go 文件为：[server.go](../../pkg/cli/command/curvebs/list/server/server.go)。
该命令的输出为：

```shell
curve bs list server                 
+----+---------------------+------+---------+-------------------+-------------------+
| ID |      HOSTNAME       | ZONE | PHYPOOL |   INTERNALADDR    |   EXTERNALADDR    |
+----+---------------------+------+---------+-------------------+-------------------+
| 1  | pubbeta2-curve9_0_0 | 1    | 1       | 10.182.26.48:8300 | 10.182.26.48:8300 |
+----+---------------------+------+         +-------------------+-------------------+
| 2  | pubbeta2-curve9_1_0 | 2    |         | 10.182.26.48:8301 | 10.182.26.48:8301 |
+----+---------------------+------+         +-------------------+-------------------+
| 3  | pubbeta2-curve9_2_0 | 3    |         | 10.182.26.48:8302 | 10.182.26.48:8302 |
+----+---------------------+------+---------+-------------------+-------------------+
```

下面是中间命令 `list` (pkg/cli/command/curvebs/list/list.go) 中的部分代码：

```golang{.line-numbers}
type ListCommand struct {
 basecmd.MidCurveCmd # 中间命令接口
}

var _ basecmd.MidCurveCmdFunc = (*ListCommand)(nil) // check interface

func (listCmd *ListCommand) AddSubCommands() { 
 # 添加子命令
 listCmd.Cmd.AddCommand(
  logicalpool.NewLogicalPoolCommand(),
  server.NewServerCommand(),  # 添加子命令 server 
 )
}

func NewListCommand() *cobra.Command {
 listCmd := &ListCommand{
  basecmd.MidCurveCmd{
   Use:   "list", # 命令名
   Short: "list resources in the curvebs", # 命令的用途
  },
 }
 return basecmd.NewMidCurveCli(&listCmd.MidCurveCmd, listCmd)
}
```

类 ListCommand 继承接口 `basecmd.MidCurveCmd`，表示它是一个中间命令；`func (listCmd *ListCommand) AddSubCommands() {...}` 用来添加子命令，该条命令的子命令包括在 `logicalpool` 、 `server` 等包下各自 New 函数返回的 cobra.Command 命令。中间命令的子命令可以是中间命令或最终命令，但最终会以最终命令结束。

下面是最终命令 `server` (pkg/cli/command/curvebs/list/server/server.go) 中的 rpc 相关的部分代码：

```golang{.line-numbers}
# list zone Server rpc 请求
type ListServerRpc struct {
 Info           *basecmd.Rpc
 Request        *topology.ListZoneServerRequest
 topologyClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*ListServerRpc)(nil) // check interface

func (lRpc *ListServerRpc) NewRpcClient(cc grpc.ClientConnInterface) {
 lRpc.topologyClient = topology.NewTopologyServiceClient(cc)
}

func (lRpc *ListServerRpc) Stub_Func(ctx context.Context) (interface{}, error) {
 return lRpc.topologyClient.ListZoneServer(ctx, lRpc.Request)
}
```

上述代码定义了发送 rpc 需要的接口和数据结构，下文可以根据需要调用  `basecmd.GetRpcListResponse` 或者 `basecmd.GetRpcResponse` 来获取这些 rpc 的请求。

`server` 是一个最终命令，需要实现 `FinalCurveCmd` 接口，接口的定义如下：

```golang{.line-numbers}
type FinalCurveCmdFunc interface {
 Init(cmd *cobra.Command, args []string) error
 RunCommand(cmd *cobra.Command, args []string) error
 Print(cmd *cobra.Command, args []string) error
 // result in plain format string
 ResultPlainOutput() error
 AddFlags()
}
```

下面是关于 `server` 命令的相关代码，分别实现了 `FinalCurveCmd` 所需的接口。其中 `NewServerCommand()` 就是上文提到的返回 cobra.Command 的 New 函数，它会返回一个 cobra.Command 对象供上层命令 `list` 调用，当输入 `curve bs list server` 时就会调用。

```golang{.line-numbers}
type ServerCommand struct {
 basecmd.FinalCurveCmd # final 命令接口
 Rpc []*ListServerRpc
}

var _ basecmd.FinalCurveCmdFunc = (*ServerCommand)(nil) // check interface

func NewServerCommand() *cobra.Command {
 return NewListServerCommand().Cmd
}

func NewListServerCommand() *ServerCommand {
 ...
 basecmd.NewFinalCurveCli(&lsCmd.FinalCurveCmd, lsCmd)
 return lsCmd
}

# 增加执行命令需要的参数
func (pCmd *ServerCommand) AddFlags() {
...
}

# 初始化工作
func (pCmd *ServerCommand) Init(cmd *cobra.Command, args []string) error {
 ...
}

func (pCmd *ServerCommand) Print(cmd *cobra.Command, args []string) error {
 return output.FinalCmdOutput(&pCmd.FinalCurveCmd, pCmd)
}

func (pCmd *ServerCommand) RunCommand(cmd *cobra.Command, args []string) error {
...
}
...
```

`ServerCommand` 的 `Init` 函数负责一些初始化的工作，主要负责构建解析参数、构建 rpc 请求、设置输出的标题等等工作，通过 Init 可以获取命令执行必要的数据。

```golang{.line-numbers}
# 调用其他命令获取必须的参数
 zones, err := zone.ListZone(pCmd.Cmd)
...

# 构建 rpc 请求
 for _, zone := range zones {
   ...
 }

 # 设置plain输出的标题 
 header := []string{cobrautil.ROW_ID, cobrautil.ROW_HOSTNAME, 
  cobrautil.ROW_ZONE, cobrautil.ROW_PHYPOOL, cobrautil.ROW_INTERNAL_ADDR,
  cobrautil.ROW_EXTERNAL_ADDR,
 }
 pCmd.SetHeader(header)
 # 设置plain输出可以合并的项
 pCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
  pCmd.Header, []string{cobrautil.ROW_PHYPOOL, cobrautil.ROW_ZONE},
 ))
```

`RunCommand` 负责具体的命令执行，发送 rpc 并解析 response，将数据填充到对应的结构中以供输出。

```golang{.line-numbers}
# 发送 rpc 请求 
 results, errs := basecmd.GetRpcListResponse(infos, funcs)
...
# 根据 rpc response 设置相关数据
 for _, res := range results {
  infos := res.(*topology.ListZoneServerResponse).GetServerInfo()
  for _, info := range infos {
   ...
  }
 }
 # 将数据转换成plain的输出接受的数据类型并添加
 list := cobrautil.ListMap2ListSortByKeys(rows, pCmd.Header, []string {
  cobrautil.ROW_PHYPOOL, cobrautil.ROW_ZONE,
 })
 pCmd.TableNew.AppendBulk(list)
 # 设置错误和json格式输出
 errRet := cmderror.MergeCmdError(errors)
 pCmd.Error = &errRet
 pCmd.Result = results
 return nil
```

## Curve 命令开发调试

> 注意：linux内核版本最好是3.15以上，且具有nbd模块，若当前内核不提供 nbd 模块，用户需自行编译并导入。
>
> 通过以下命令查看内核版本：
>
> ```shell
> uname -r
> ```
>
> 推荐操作系统：debian10、11。

### 部署 Curve 集群

首先你需要部署一个 Curve 集群，curve集群拉起方式如下：

1. 安装curveadm:

```shell
CURVEADM_VERSION=v0.1.12-dev bash -c "$(curl -fsSL https://curveadm.nos-eastchina1.126.net/script/install.sh)"
```

2. 执行 playground 命令时得确保当前用户有 root 权限，或者给 docker 的 socket 加上任意用户读写权限，或者将用户加入 docker 用户组：

```shell
 curveadm playground run --kind curvebs --container_image harbor.cloud.netease.com/curve/curvebs:playground
# ls -la /var/run/docker.sock
srwxrwxrwx 1 root docker 0 Oct 26 17:57 /var/run/docker.sock

# 加入用户组
sudo usermod -aG docker $USER
```

> 相关文档可参考：
>
> 1. [Post-installation steps for Linux](https://docs.docker.com/engine/install/linux-postinstall/)
>
> 2. [Run the Docker daemon as a non-root user (Rootless mode)](https://docs.docker.com/engine/security/rootless/)

### 环境准备

1. 安装 [golang 1.19](https://go.dev/doc/install) 版本及以上
2. 安装 [protoc-v21.8](https://github.com/protocolbuffers/protobuf/releases/tag/v21.8)，请保证命令 `protoc` 可执行
3. 安装 grpc 及相关插件（也可以在项目目录 `tools-v2` 下执行 `make install_grpc_protobuf`）

```shell
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

> 以上过程可能安装缓慢，需要设置代理：
> ```shell
> go env -w  GOPROXY=https://goproxy.io,direct
> ```

### 编译

在 tools-v2 目录下执行 `make debug` 即可完成编译：

```shell
make
```

生成的二进制文件保存为 `tools-v2/sbin/curve`。

> 可能遇到的问题-1：找不到 `protoc-gen-go` 、`protoc-gen-go-grpc` 二进制文件，但在上面步骤已经install。
>
> 解决办法-1：将 `~/go` 添加进环境变量 `PATH` ：
>
> ```shell
> export $PATH=$HOME/go/bin:$PATH
> ```
>
> 解决办法-2：拷贝二进制文件或创建软链接。
>
> 查看 `GOROOT` 下（一般为 `~/go` ）是否有二进制文件，若有，可以将其拷贝至go的安装路径后再重新编译。
>
> ```shell
> cd ~/go/bin
> cp ./protoc-gen-go* /usr/local/go/bin
> ```
>
> ---
>
> 可能遇到的问题-2：出现musl-gcc相关的报错。
>
> 解决办法-1：[安装musl-gcc](https://command-not-found.com/musl-gcc)。
>
> 解决办法-2：直接编译：
>
> ```shell
> go build -o sbin/curve ./cmd/curve/main.go
> ```

### 调试

你可以通过一下两种方式来对生成的二进制文件进行调试：

1. gdb
2. [delve](https://github.com/go-delve/delve)。

可以通过以下命令使用 delve 来对二进制程序 `sbin/curve` 来调试：

```shell
dlv exec sbin/curve --${命令行参数}
```

### 调试流程

1. 检查环境是否拉起成功，记录容器ID，后续有用：

   ```shell
   docker ps -a
   ```

2. 编写好代码后，在 /curve/tools-v2 目录下编译成二进制文件。

   ```shell
   make
   ```

3. 将编译好的 Curve 文件拷贝进 playground 容器内：

   ```shell
   docker cp ./sbin/curve de7603f17cf9:/
   ```

4. 准备配置文件，将之拷贝进 playground 容器内：

   ```shell
   docker cp /pkg/config/template.yaml de7603f17cf9:/etc/curve/curve.yaml
   ```

5. 进入对应的容器：

   ```shell
   docker exec -it de7603f17cf9 bash
   ```

6. 执行命令/调试。

   > 查看状态：
   >
   > ```shell
   > ./curve bs status mds
   > ```
   >
   > 新建一个目录：
   >
   > ```shell
   > ./curve bs create dir --path /yourname
   > ```
   >
   > 查看刚刚新建的目录，可以发现新建的目录已经添加：
   >
   > ```shell
   > ./curve bs list dir --dir /
   > ```

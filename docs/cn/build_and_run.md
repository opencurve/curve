[English version](../en/build_and_run_en.md)

# 编译环境搭建

**请注意：**

1. 如您只是想体验Curve的部署流程和基本功能，**则不需要编译Curve**，请参考 [部署](https://github.com/opencurve/curveadm/wiki)
2. 本文档仅用来帮助你搭建Curve代码编译环境，便于您参与Curve的开发调试
3. 以下镜像和编译过程目前仅支持 x86 系统
4. 如要编译[arm分支](https://github.com/opencurve/curve/pull/2408)，请根据 [Dockerfile](https://github.com/opencurve/curve/blob/master/docker/debian9/compile/Dockerfile)打包编译镜像
5. 目前master分支不支持在arm系统上编译运行
6. 推荐 debian 10及以上版本的操作系统

## 使用Docker进行编译（推荐方式）

### 获取或者构建docker镜像

方法一：从docker hub镜像库中拉取docker镜像（推荐方式）

```bash
docker pull opencurvedocker/curve-base:build-debian9
```

方法二：手动构建docker镜像

使用工程目录下的 docker/debian9/compile/Dockerfile 进行构建，命令如下：

```bash
docker build -t opencurvedocker/curve-base:build-debian9
```

**注意：** 上述操作不建议在Curve工程目录执行，否则构建镜像时会把当前目录的文件都复制到docker镜像中，建议把Dockerfile拷贝到新建的干净目录下进行docker镜像的构建。

### 在docker镜像中编译

```bash
git clone https://github.com/opencurve/curve.git 或者 git clone https://gitee.com/mirrors/curve.git
cd curve
# 如果你想在容器内完成编译+制作+上传镜像的操作，可以添加以下参数
# -v /var/run/docker.sock:/var/run/docker.sock -v /root/.docker:/root/.docker
# --rm 会在容器退出后自动删除容器,如果你想保留容器，可以去掉该参数
docker run --rm -v $(pwd):/curve -w /curve -v ${HOME}/.cache:${HOME}/.cache -v ${HOME}/go:${HOME}/go --user $(id -u ${USER}):$(id -g ${USER}) -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro --privileged -it opencurvedocker/curve-base:build-debian9 bash
# （中国大陆可选）将外部依赖替换为国内下载点或镜像仓库，可以加快编译速度： bash replace-curve-repo.sh

# curve v2.0 之前
bash mk-tar.sh （编译 curvebs 并打tar包）
bash mk-deb.sh （编译 curvebs 并打debian包）

# （当前）curve v2.0 及之后
# 编译 curvebs:
make build stor=bs dep=1
# or
make dep stor=bs && make build stor=bs
# 编译 curvefs:
make build stor=fs dep=1
# or
make dep stor=fs && make build stor=fs
```

**注意：** `mk-tar.sh` 和 `mk-deb.sh` 用于 curve v2.0 之前版本的编译打包，v2.0 版本之后不再维护。

## 在物理机上编译

Curve编译依赖的包括：

| 依赖 | 版本 |
|:-- |:-- |
| bazel | 4.2.2 |
| gcc   | 支持c++11的兼容版本 |

Curve的其他依赖项，均由bazel去管理，不可单独安装。

**注意：** 4.* 版本的 bazel 均可以成功编译 Curve 项目，其他版本不兼容。
4.2.2 为推荐版本。

### 安装依赖

编译相关的软件依赖可以参考 [dockerfile](../../docker/debian9/compile/Dockerfile) 中的安装步骤。

### 一键编译

```bash
git clone https://github.com/opencurve/curve.git 或者 git clone https://gitee.com/mirrors/curve.git
# （中国大陆可选）将外部依赖替换为国内下载点或镜像仓库，可以加快下载速度： bash replace-curve-repo.sh
# curve v2.0 之前
bash mk-tar.sh （编译 curvebs 并打tar包）
bash mk-deb.sh （编译 curvebs 并打debian包）

# （当前）curve v2.0 及之后
# 编译 curvebs:
make build stor=bs dep=1
# or
make dep stor=bs && make build stor=bs
# 编译 curvefs: 
make build stor=fs dep=1
# or
make dep stor=fs && make build stor=fs
```
### 制作镜像

该步骤可以在容器内执行也可以在物理机上执行。
注意若是在容器内执行，需要在执行 `docker run` 命令时添加 `-v /var/run/docker.sock:/var/run/docker.sock -v /root/.docker:/root/.docker` 参数。

```bash
# 编译 curvebs:
# 后面的tag参数可以自定义，用于上传到镜像仓库
make image stor=bs tag=test
# 编译 curvefs: 
make image stor=fs tag=test
```

### 上传镜像

```bash
# test 为上一步中的tag参数
docker push test
```

## 测试用例编译及执行

### 编译全部模块

仅编译全部模块，不进行打包
```bash
bash ./build.sh
```

### 列出所有测试模块

```bash
# curvebs
bazel query '//test/...'
# curvefs
bazel query '//curvefs/test/...'
```

### 编译对应模块的代码

编译对应模块，例如test/common目录下的common-test测试：

```bash
bazel build test/common:common-test --copt -DHAVE_ZLIB=1 --define=with_glog=true --compilation_mode=dbg --define=libunwind=true
```

### 执行测试

执行测试前需要先准备好测试用例运行所需的依赖：

运行单元测试:
- 构建对应的模块测试:
  ```bash
  $ bazel build xxx/...//:xxx_test
  ```
- 运行对应的模块测试:
  ```bash
  $ bazel run xxx/...//:xxx_test
  # 或者
  $ ./bazel-bin/xxx/.../xxx_test
  ```
- 编译全部测试及文件
  ```bash
  $ bazel build "..."
  ```
- bazel 默认自带缓存编译, 但有时可能会失效.
  
  清除项目构建缓存:
  ```bash
  $ bazel clean
  ```
  
  清除项目依赖缓存(bazel 会将WORKSPACE 文件中的指定依赖项自行编译, 这部分同样也会缓存):
  ```bash
  $ bazel clean --expunge
  ```
- debug 模式编译(-c 指定向bazel 传递参数), 该模式会在默认构建文件中加入调试符号, 及减少优化等级.
  ```bash
  $ bazel build xxx//:xxx_test -c dbg
  ```
- 优化模式编译
  ```bash
  $ bazel build xxx//:xxx_test -c opt
  # 优化模式下加入调试符号
  $ bazel build xxx//:xxx_test -c opt --copt -g
  ```
- 更多文档, 详见 [bazel docs](https://bazel.build/docs).

#### 动态库

```bash
export LD_LIBRARY_PATH=<CURVE-WORKSPACE>/thirdparties/etcdclient:<CURVE-WORKSPACE>/thirdparties/aws-sdk/usr/lib:/usr/local/lib:${LD_LIBRARY_PATH}
```

#### fake-s3

快照克隆集成测试中，使用了开源的[fake-s3](https://github.com/jubos/fake-s3)模拟真实的s3服务。

```bash
$ apt install ruby -y OR yum install ruby -y
$ gem install fakes3
$ fakes3 -r /S3_DATA_DIR -p 9999 --license YOUR_LICENSE_KEY
```

备注：

- `-r S3_DATA_DIR`：存放数据的目录
- `--license YOUR_LICENSE_KEY`：fakes3需要key才能运行，申请地址见[fake-s3](https://github.com/jubos/fake-s3)
- `-p 9999`：fake-s3服务启动的端口，**不用更改**

#### etcd

```bash
wget -ct0 https://github.com/etcd-io/etcd/releases/download/v3.4.10/etcd-v3.4.10-linux-amd64.tar.gz
tar zxvf etcd-v3.4.10-linux-amd64.tar.gz
cd etcd-v3.4.10-linux-amd64 && cp etcd etcdctl /usr/bin
```

#### 执行单个测试模块

```bash
./bazel-bin/test/common/common-test
```

#### 运行单元/集成测试

bazel 编译后的可执行程序都在 `./bazel-bin` 目录下，例如 test/common 目录下的测试代码对应的测试程序为 `./bazel-bin/test/common/common-test`，可以直接运行程序进行测试。
- CurveBS相关单元测试程序目录在 ./bazel-bin/test 目录下
- CurveFS相关单元测试程序目录在 ./bazel-bin/curvefs/test 目录下
- 集成测试在 ./bazel-bin/test/integration 目录下
- NEBD相关单元测试程序在 ./bazel-bin/nebd/test 目录下
- NBD相关单元测试程序在 ./bazel-bin/nbd/test 目录下

如果想运行所有的单元测试和集成测试，可以执行工程目录下的ut.sh脚本：

```bash
bash ut.sh
```

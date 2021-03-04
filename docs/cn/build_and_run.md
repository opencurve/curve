[English version](../en/build_and_run_en.md)

# 编译环境搭建

请注意：
1. 如您只是想体验CURVE的部署流程和基本功能，**则不需要编译CURVE**，请参考[单机部署](deploy.md#%E5%8D%95%E6%9C%BA%E9%83%A8%E7%BD%B2)
2. 本文档仅用来帮助你搭建CURVE代码编译环境，便于您参与CURVE的开发调试及打包测试

## 使用Docker进行编译（推荐方式）

### 获取或者构建docker镜像

方法一：从docker hub镜像库中拉取docker镜像（推荐方式）

```bash
docker pull opencurve/curvebuild:centos8
```

方法二：手动构建docker镜像

使用工程目录下的Dockerfile进行构建，命令如下：

```bash
docker build -t opencurve/curvebuild:centos8 .
```

注意：上述操作不建议在CURVE工程目录执行，否则构建镜像时会把当前目录的文件都复制到docker镜像中，建议把Dockerfile拷贝到新建的干净目录下进行docker镜像的构建。



### 在docker镜像中编译

```bash
docker run -it opencurve/curvebuild:centos8 /bin/bash
cd <workspace>
git clone https://github.com/opencurve/curve.git 或者 git clone https://gitee.com/mirrors/curve.git
# （可选步骤）将外部依赖替换为国内下载点或镜像仓库，可以加快编译速度： bash replace-curve-repo.sh
bash mk-tar.sh
```

基于tar包的安装部署流程可参考：[集群部署](deploy.md)

## 在物理机上编译

CURVE编译依赖的包括：

| 依赖 | 版本 |
|:-- |:-- |
| bazel | 0.17.2 |
| gcc   | 支持c++11的兼容版本 |

CURVE的其他依赖项，均由bazel去管理，可不单独安装。

### 安装依赖

#### Ubuntu/LinuxMint/WSL

```
sudo apt-get install -y bazel git g++ make
```


#### Fedora/CentOS

```
sudo yum install bazel git gcc-c++ make
```

### 一键编译和打包

CURVE提供一键编译脚本，mk-tar.sh 生成所需的全部tar二进制包，命令如下：

```
# （可选步骤）将外部依赖替换为国内下载点或镜像仓库，可以加快编译速度： bash replace-curve-repo.sh
bash ./mk-tar.sh
```

基于tar包的安装部署流程可参考：[集群部署](deploy.md)

特别的，由于CURVE内部版本使用在debian系统上，因此特别提供debian的版本，命令如下：

```
bash ./mk-deb.sh
```
注意：基于deb包的安装部署流程正在整理中，目前不推荐使用deb包安装部署

仅编译全部模块，不进行打包，可以执行命令：

```
bash ./build.sh
```

## 测试用例编译及执行

### 编译全部模块

仅编译全部模块，不进行打包
```
bash ./build.sh
```

### 编译对应模块的代码和运行测试

编译对应模块，例如test/common目录下的common-test测试：

```
bazel build test/common:common-test --copt -DHAVE_ZLIB=1 --define=with_glog=true --compilation_mode=dbg --define=libunwind=true
```

### 执行测试

执行测试前需要先准备好测试用例运行所需的依赖：

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
```
./bazel-bin/test/common/common-test
```

#### 运行单元/集成测试

bazel 编译后的可执行程序都在 `./bazel-bin` 目录下，例如 test/common 目录下的测试代码对应的测试程序为 `./bazel-bin/test/common/common-test`，可以直接运行程序进行测试。
- CURVE相关单元测试程序目录在 ./bazel-bin/test 目录下，集成测试在 ./bazel-bin/test/integration 目录下
- NEBD相关单元测试程序在 ./bazel-bin/nebd/test 目录下
- NBD相关单元测试程序在 ./bazel-bin/nbd/test 目录下

如果想运行所有的单元测试和集成测试，可以执行工程目录下的ut.sh脚本：

```bash
bash ut.sh
```

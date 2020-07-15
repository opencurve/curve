# 编译与运行

## 依赖项

curve编译依赖的包括：

| 依赖 | 版本 |
|:-- |:-- |
| bazel | 0.17.2 |
| gcc   | 支持c++14的兼容版本，推荐 >= 6.1.0 |

curve的其他依赖项，均有bazel去管理，可不单独安装。

## 安装依赖

### Ubuntu/LinuxMint/WSL

```
sudo apt-get install -y bazel git g++ make
```


### Fedora/CentOS

```
sudo yum install bazel git gcc-c++ make
```

## 编译

### 一键编译和打包

opencurve 提供一键编译脚本，mk-tar.sh 生成所需的全部tar二进制包，命令如下：

```
bash ./mk-tar.sh
```

特别的，由于curve内部版本使用在debian系统上，因此特别提供debian的版本，命令如下：

```
bash ./mk-deb.sh
```

### 单独编译和运行模块测试

#### 编译全部模块

仅编译全部模块，不进行打包
```
bazel build ... --compilation_mode=dbg -s --collect_code_coverage  --jobs=32 --copt   -DHAVE_ZLIB=1 --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX
```

#### 编译对应模块的代码和运行测试·

编译对应模块，例如test/common目录下的common-test测试：

```
bazel build test/common:common-test --copt -DHAVE_ZLIB=1 --define=with_glog=true --compilation_mode=dbg --define=libunwind=true
```

单独运行该测试：

```
./bazel-bin/test/common/common-test
```



















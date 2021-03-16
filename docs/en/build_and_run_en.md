[中文版](../cn/build_and_run.md)

# Build compilation environment

**Note:**
1. If you just want to experience the deployment and basic functions of CURVE, **you do not need to compile CURVE**, please refer to [Single-machine deployment](deploy_en.md).
2. This document is only used to help you build the CURVE code compilation environment, which is convenient for you to participate in the development, debugging and packaging test of CURVE.

## Compile with docker (recommended)

### Get or build docker image

Method 1: Pull the docker image from the docker hub image library (recommended)

```bash
$ docker pull opencurve/curvebuild:centos8
```

Method 2: Build docker image manually

Use the Dockerfile in the project directory to build. The command is as follows:

```bash
$ docker build -t opencurve/curvebuild:centos8.
```

Note: The above operations are not recommended to be performed in the CURVE project directory, otherwise the files in the current directory will be copied to the docker image when building the image. It is recommended to copy the Dockerfile to the newly created clean directory to build the docker image.


### Compile in docker image

```bash
$ docker run -it opencurve/curvebuild:centos8 /bin/bash
$ cd <workspace>
$ git clone https://github.com/opencurve/curve.git
$ bash mk-tar.sh
```

For the installation and deployment process based on the tar package, please refer to: [Cluster Deployment](deploy_en.md).

## Compile on a physical machine

CURVE compilation depends on:

| Dependency | Version |
|:-- |:-- |
| bazel | 0.17.2 |
| gcc   | Compatible version supporting C++11 |

Other dependencies of CURVE are managed by bazel and do not need to be installed separately.

### Installation dependency

#### Ubuntu/LinuxMint/WSL

```
$ sudo apt-get install -y bazel git g++ make
```


#### Fedora/CentOS

```
$ sudo yum install bazel git gcc-c++ make
```

### One-click compilation and packaging

CURVE provides a one-click compilation script, [mk-tar.sh](https://github.com/opencurve/curve/blob/master/mk-tar.sh) generates all the required tar binary packages, the command is as follows:

```
$ bash ./mk-tar.sh
```

For the installation and deployment process based on the tar package, please refer to: [Cluster Deployment](deploy_md.md).

In particular, since the internal version of CURVE is used on the Debian system, the Debian version is specially provided. The command is as follows:

```
$ bash ./mk-deb.sh
```

Note: The installation and deployment process based on the deb package is being sorted out. Currently, it is not recommended to use the deb package to install and deploy.


Only compile all modules without packaging, you can execute the command:

```
$ bash ./build.sh
```

## Test case compilation and execution

### Compile all modules

Only compile all modules without packaging

```
$ bash ./build.sh
```

### Compile the corresponding module code and run the test

Compile corresponding modules, such as common-test in the `test/common` directory

```
$ bazel build test/common:common-test --copt -DHAVE_ZLIB=1 \
$    --define=with_glog=true --compilation_mode=dbg \
$    --define=libunwind=true
```

### Perform the test

Before executing the test, you need to prepare the dependencies required for the test case to run:

#### Dynamic library

```bash
$ export LD_LIBRARY_PATH=<CURVE-WORKSPACE>/thirdparties/etcdclient:<CURVE-WORKSPACE>/thirdparties/aws-sdk/usr/lib:/usr/local/lib:${LD_LIBRARY_PATH}
```

#### fake-s3

In the snapshot clone integration test, the open source [fake-s3](https://github.com/jubos/fake-s3) was used to simulate the real s3 service.

```bash
$ apt install ruby ​​-y OR yum install ruby ​​-y
$ gem install fakes3
$ fakes3 -r /S3_DATA_DIR -p 9999 --license YOUR_LICENSE_KEY
```

Remarks:

- `-r S3_DATA_DIR`: The directory where data is stored
- `--license YOUR_LICENSE_KEY`: fakes3 needs a key to run, please refer to [fake-s3](https://github.com/jubos/fake-s3)
- `-p 9999`: The port where the fake-s3 service starts, **no need to change**

#### etcd

```bash
$ wget -ct0 https://github.com/etcd-io/etcd/releases/download/v3.4.10/$ etcd-v3.4.10-linux-amd64.tar.gz
$ tar zxvf etcd-v3.4.10-linux-amd64. tar.gz
$ cd etcd-v3.4.10-linux-amd64 && cp etcd etcdctl /usr/bin
```

#### Execute a single test module

```
$ ./bazel-bin/test/common/common-test
```

#### Run unit/integration tests

The executable programs compiled by bazel are all in the `./bazel-bin` directory, for example, the test program corresponding to the test code in the test/common directory is `./bazel-bin/test/common/common-test`, this program can be run directly for testing.
- CURVE-related unit test program directory is under the `./bazel-bin/test` directory, and the integration test is under the `./bazel-bin/test/integration` directory
- NEBD-related unit test programs are in the `./bazel-bin/nebd/test` directory
- NBD-related unit test programs are in the `./bazel-bin/nbd/test` directory

If you want to run all unit tests and integration tests, you can execute the ut.sh script in the project directory:

```bash
$ bash ut.sh
```
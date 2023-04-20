[中文版](../cn/build_and_run.md)

# Build compilation environment

**Note:**
1. If you just want to experience the deployment and basic functions of Curve, **you do not need to compile Curve**, please refer to [deployment](https://github.com/opencurve/curveadm/wiki).
2. This document is only used to help you build the Curve code compilation environment, which is convenient for you to participate in the development, debugging and run tests of Curve.
3. The following image and build procedures are currently only supported on x86 systems.
4. To compile [arm branch](https://github.com/opencurve/curve/pull/2408), please follow [Dockerfile](https://github.com/opencurve/curve/blob/master/docker/debian9/compile/Dockerfile) to package and compile the image.
5. Currently the master branch does not support compiling and running on the arm system

## Compile with docker (recommended)

### Get or build docker image

Method 1: Pull the docker image from the docker hub image library (recommended)

```bash
docker pull opencurvedocker/curve-base:build-debian9
```

Method 2: Build docker image manually

Use the Dockerfile in the project directory to build. The command is as follows:

```bash
docker build -t opencurvedocker/curve-base:build-debian9
```

**Note:** The above operations are not recommended to be performed in the Curve project directory, otherwise the files in the current directory will be copied to the docker image when building the image. It is recommended to copy the Dockerfile to the newly created clean directory to build the docker image.

### Compile in docker image

```bash
docker run -it opencurvedocker/curve-base:build-debian9 /bin/bash
cd <workspace>
git clone https://github.com/opencurve/curve.git or git clone https://gitee.com/mirrors/curve.git
# (Optional for Chinese mainland) Replace external dependencies with domestic download points or mirror warehouses, which can speed up compilation： bash replace-curve-repo.sh
# before curve v2.0
bash mk-tar.sh （compile curvebs and make tar package）
bash mk-deb.sh （compile curvebs and make debian package）
# (current) after curve v2.0
compile curvebs: cd curve && make build stor=bs dep=1
compile curvefs: cd curve && make build stor=fs dep=1
```

**Note:** `mk-tar.sh` and `mk-deb.sh` are used for compiling and packaging curve v2.0. They are no longer maintained after v2.0.

## Compile on a physical machine

Curve compilation depends on:

| Dependency | Version |
|:-- |:-- |
| bazel | 4.2.2 |
| gcc   | Compatible version supporting C++11 |

Other dependencies of Curve are managed by bazel and do not need to be installed separately.

**Note:** The 4.* version of bazel can successfully compile the curve project, other versions are not compatible.
4.2.2 is the recommended version.

### Installation dependency

For dependencies, you can refer to the installation steps in [dockerfile](../../docker/debian9/compile/Dockerfile).

### One-click compilation

```bash
git clone https://github.com/opencurve/curve.git or git clone https://gitee.com/mirrors/curve.git
# (Mainland China optional) Replace external dependencies with domestic download points or mirror warehouses, which can speed up compilation： bash replace-curve-repo.sh
# before curve v2.0
bash mk-tar.sh （compile curvebs and make tar package）
bash mk-deb.sh （compile curvebs and make debian package）
# (current) after curve v2.0
compile curvebs: cd curve && make build stor=bs dep=1
compile curvefs: cd curve && make build stor=fs dep=1
```

## Test case compilation and execution

### Compile all modules

Only compile all modules without packaging

```bash
$ bash ./build.sh
```

### List all test modules

```bash
# curvebs
bazel query '//test/...'
# curvefs
bazel query '//curvefs/test/...'
```

### Compile the corresponding module code

Compile corresponding modules, such as common-test in the `test/common` directory

```bash
$ bazel build test/common:common-test --copt -DHAVE_ZLIB=1 \
$    --define=with_glog=true --compilation_mode=dbg \
$    --define=libunwind=true
```

### Perform the test

Before executing the test, you need to prepare the dependencies required for the test case to run:

execute unit tests:
- build module tests:
  ```bash
  $ bazel build xxx/...//:xxx_test
  ```

- run module tests:

  ```bash
  $ bazel run xxx/xxx//:xxx_test
  ```

- compile all tests

  ```bash
  $ bazel build "..."
  ```

- sometimes the bazel compiling cache will be failure.

  clean the project cache:
  ```bash
  $ bazel clean
  ```

  clean the project cache and deps cache.(bazel will also save project cache).
  ```bash
  $ bazel clean --expunge
  ```

- debug mode build:
  ```bash
  $ bazel build xxx//:xxx_test -c dbg
  ```

- releases mode build
  ```bash
  $ bazel build xxx//:xxx_test -c opt
  ```

- more about bazel docs, please go [bazel docs](https://bazel.build/docs).

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

```bash
$ ./bazel-bin/test/common/common-test
```

#### Run unit/integration tests

The executable programs compiled by bazel are all in the `./bazel-bin` directory, for example, the test program corresponding to the test code in the test/common directory is `./bazel-bin/test/common/common-test`, this program can be run directly for testing.
- CurveBS-related unit test program directory is under the `./bazel-bin/test` directory
- CurveFS-related unit test program directory is under the `./bazel-bin/curvefs/test` directory
- The integration test is under the `./bazel-bin/test/integration` directory
- NEBD-related unit test programs are in the `./bazel-bin/nebd/test` directory
- NBD-related unit test programs are in the `./bazel-bin/nbd/test` directory

If you want to run all unit tests and integration tests, you can execute the ut.sh script in the project directory:

```bash
$ bash ut.sh
```

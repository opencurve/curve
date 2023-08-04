[中文版](../cn/build_and_run.md)

- [Build compilation environment](#build-compilation-environment)
  - [Compile with docker (recommended)](#compile-with-docker-recommended)
    - [Get or build docker image](#get-or-build-docker-image)
    - [Compile in docker image](#compile-in-docker-image)
  - [Compile on a physical machine(not recommended)](#compile-on-a-physical-machinenot-recommended)
    - [Installation dependency](#installation-dependency)
    - [One-click compilation](#one-click-compilation)
  - [Make a mirror image](#make-a-mirror-image)
  - [Upload image](#upload-image)
  - [Test](#test)
    - [Test in docker container](#test-in-docker-container)
  - [Test on physical machine](#test-on-physical-machine)
  - [Test case compilation and execution](#test-case-compilation-and-execution)
    - [Compile all modules](#compile-all-modules)
    - [List all test modules](#list-all-test-modules)
    - [Compile the corresponding module code](#compile-the-corresponding-module-code)
    - [Perform the test](#perform-the-test)
      - [Execute a single test module](#execute-a-single-test-module)
      - [Run unit/integration tests](#run-unitintegration-tests)

# Build compilation environment

**Note:**
1. If you just want to experience the deployment and basic functions of Curve, **you do not need to compile Curve**, please refer to [deployment](https://github.com/opencurve/curveadm/wiki).
2. This document is only used to help you build the Curve code compilation environment, which is convenient for you to participate in the development, debugging and run tests of Curve.
3. The following image and build procedures are currently only supported on x86 systems.
4. To compile [arm branch](https://github.com/opencurve/curve/pull/2408), please follow [Dockerfile](https://github.com/opencurve/curve/blob/master/docker/debian9/compile/Dockerfile) to package and compile the image.
5. Currently the master branch does not support compiling and running on the arm system
6. Recommend using Debian 10 or later versions of the operating system. Other operating systems have not been thoroughly tested.

## Compile with docker (recommended)

### Get or build docker image

Method 1: Pull the docker image from the docker hub image library (recommended)

```bash
docker pull opencurvedocker/curve-base:build-debian11
```

Method 2: Build docker image manually

Use the Dockerfile in the project directory to build. The command is as follows:

```bash
docker build -t opencurvedocker/curve-base:build-debian11
```

**Note:** The above operations are not recommended to be performed in the Curve project directory, otherwise the files in the current directory will be copied to the docker image when building the image. It is recommended to copy the Dockerfile to the newly created clean directory to build the docker image.

### Compile in docker image

**Note:** The `make docker` and `make build` commands below will start a temporary container that will be automatically deleted after exiting (`--rm`). The command will map some directories to the container. If you have concerns or need some custom settings, you can read and modify `util/docker.sh` by yourself

```bash
git clone https://github.com/opencurve/curve.git 或者 git clone https://gitee.com/mirrors/curve.git
cd curve
# (Optional for Chinese mainland) Replace external dependencies with domestic download points or mirror warehouses, which can speed up compilation： bash replace-curve-repo.sh

# before curve v2.0
bash mk-tar.sh （compile curvebs and make tar package）
bash mk-deb.sh （compile curvebs and make debian package）

# (current) after curve v2.0
# compile curvebs:
make build stor=bs dep=1
# or 
make dep stor=bs && make build stor=bs
# compile curvefs:
make build stor=fs dep=1
# or 
make dep stor=fs && make build stor=fs
```

You can also use the `make docker` command to enter the container, and then execute the compilation commands in the container for manual compilation.
The `make ci-build` command compiles directly without entering the container.

```bash
make docker

# bs
make ci-build stor=bs dep=1
# or 
make ci-dep stor=bs && make ci-build stor=bs
# fs
make ci-build stor=fs dep=1
# or
make ci-dep stor=fs && make ci-build stor=fs
```

**Note:** `mk-tar.sh` and `mk-deb.sh` are used for compiling and packaging curve v2.0. They are no longer maintained after v2.0.

## Compile on a physical machine(not recommended)

Curve compilation depends on:

| Dependency | Version |
|:-- |:-- |
| bazel | 4.2.2 |
| gcc   | Compatible version supporting C++11 |

Other dependencies of Curve are managed by bazel and do not need to be installed separately.

**Note:** The 4.* version of bazel can successfully compile the curve project, other versions are not compatible.
4.2.2 is the recommended version.

### Installation dependency

For dependencies, you can refer to the installation steps in [dockerfile](../../docker/debian11/compile/Dockerfile).

### One-click compilation

```bash
git clone https://github.com/opencurve/curve.git or git clone https://gitee.com/mirrors/curve.git
# (Mainland China optional) Replace external dependencies with domestic download points or mirror warehouses, which can speed up compilation： bash replace-curve-repo.sh
# before curve v2.0
bash mk-tar.sh （compile curvebs and make tar package）
bash mk-deb.sh （compile curvebs and make debian package）

# (current) after curve v2.0
# compile curvebs:
make ci-build stor=bs dep=1
# or 
make ci-dep stor=bs && make ci-build stor=bs
# compile curvefs:
make ci-build stor=fs dep=1
# or
make ci-dep stor=fs && make ci-build stor=fs
```

## Make a mirror image

This step can be performed in a container or on a physical machine.
Note that if it is executed in a container, you need to add `-v /var/run/docker.sock:/var/run/docker.sock -v /root/.docker:/root/.docker when executing the `docker run` command ` parameter.

```bash
# Compile curvebs:
# The following tag parameter can be customized for uploading to the mirror warehouse
make image stor=bs tag=test
# Compile curvefs:
make image stor=fs tag=test
```

## Upload image

```bash
# test is the tag parameter in the previous step
docker push test
```

## Test

### Test in docker container

The docker image opencurvedocker/curve-base:build-debian11 has installed all the dependencies required for testing, so testing can be performed directly in the container.

Run the following command to start a container and execute all curvebs related ci tests in the container:
  
```bash
bash ut.sh curvebs
```

Similarly, you can also use the `make docker` command to enter the container, and then execute test commands in the container (for example, when you want to test specific test cases separately).

Like compiling, you can also use the `make docker` command to enter the container, and then execute the test command in the container:

```bash
make docker
bash util/ut_in_image.sh curvebs
```

**Note** The script `util/ut_in_image.sh` will run minio. If it fails midway, minio needs to be terminated manually before the next run.

## Test on physical machine

Refer to [dockerfile](../../docker/debian11/compile/Dockerfile) to install dependencies.
Use the following command to run all curvebs related ci tests on the physical machine:

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

To run all unit tests and integration tests, please refer to [Test in docker container](#test-in-docker-container) and [Test on physical machine](#test-on-physical-machine).

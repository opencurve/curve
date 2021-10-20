CURVEFS
===

Curve FileSystem

Table of Contents
===
* [Requirement](#requirement)
* [Quick Start](#quick-start)
* [Devops](#devops)
* [Hint](#hint)

Requirement
===

| requirement | version | guide                               |
| :---        | :---    | :---                                |
| OS          | Linux*  |                                     |
| Bazel       | 0.17.2  | See [installation][bazel_install]   |
| Ansible     | < 2.0   | See [installation][ansible_install] |
| Fuse        | >= 3.0  | See [installation][fuse_install]    |


[Back to Toc](#table-of-contents)

Quick Start
===

step 1: clone repository, run build and install:

```
$ git clone https://github.com/opencurve/curve.git
$ cd curve/curvefs
$ make build
$ make install
$ make install only=etcd
```

step 2: edit ansible config file, inventory file and client config file:

* devops/ansible.cfg:
    * `remoter_user`: we use this user to log in to the remote machine, create file and start service
    * `private_key_file`: paste `remote_user`'s private key to `devops/ssh/pub_rsa` (you can also save the private key anywhere locally and point the `private_key_file` to it)
    * Please make sure that the mode of private key file which `private_key_file` specfied is `600` (`-rw-------`)
    * Please make sure that the public key already exists in `remote_user`'s ssh file (`~/.ssh/authorized_keys`)
* inventory/server.ini:
    * You can specify which hosts to deploy which services
    * Please specify the client mount path (`client_mount_path`), the path will create automatic if it not exist
    * Please specify the filesystem name which client mount (`client_mount_fsname`), the filesystem will create automatic if it not exist
    * Please specify the S3 related field (`s3.ak`、`s3.sk`、`s3.endpoint`、`s3.bucket_name`) which used to store data
    * The `tools` only used to create topology, you can select one of `mds` hosts

step 3: deploy all and mount curve filesystem:

```
$ make deploy
$ make mount
```

once this is done, you can enter the mount path and do anything like local filesystem.

[Back to Toc](#table-of-contents)

Devops
===

| command                                  | description                                   |
| :---                                     | :---                                          |
| make build [only=ONLY] [release=1]       | compile                                       |
| make install [only=ONLY] [prefix=PREFIX] | install                                       |
| make deploy                              | deploy all                                    |
| make core [only=ONLY] [hosts=HOSTS]      | sync binary file                              |
| make config [only=ONLY] [hosts=HOSTS]    | sync config file                              |
| make start [only=ONLY] [hosts=HOSTS]     | start service                                 |
| make stop [only=ONLY] [hosts=HOSTS]      | stop service                                  |
| make reload [only=ONLY] [hosts=HOSTS]    | restart service                               |
| make status [only=ONLY] [hosts=HOSTS]    | show service status                           |
| make clean [only=ONLY] [hosts=HOSTS]     | clean environment (include all created files) |
| make topo                                | create topology                               |
| make mount                               | mount curve filesystem                        |
| make umount                              | umount curve filesystem                       |


* If you want to execute action for specfied service, you can use `only` option, e.g: `make start only=mds`
* If you want to execute action in specfied host, you can use `hosts` option, e.g: `make start hosts=machine1`
* You can also specify both `only` and `hosts` option, e.g: `make start only=mds hosts=machine1:machine2`
* The `only` option can be one of the following values: `etcd`、`mds`、`metaserver`、`space`、`client`、 `tools`

[Back to Toc](#table-of-contents)

HINT
===

* The default install prefix of projects is `devops/projects`
* If you want to modify the service's config, you can modify the config under `conf` directory, then run `make config`
* You can use `make status` to show service status, include active status, listen address, memory usage and etc:

```
$ make status only=mds

curve-vm1:
mds.service - CurveFS Mds
    Active: [RUNNING] since 2021-09-26 19:27:39; 52:35 ago
  Main PID: 326373 (curvefs_mds)
    Daemon: True
    Listen: 0.0.0.0:26700, 0.0.0.0:27700
       Mem: 51688 KB

curve-vm2:
mds.service - CurveFS Mds
    Active: [RUNNING] since 2021-09-26 19:27:39; 52:35 ago
  Main PID: 326381 (curvefs_mds)
    Daemon: True
    Listen: 0.0.0.0:37700
       Mem: 40688 KB

curve-vm3:
mds.service - CurveFS Mds
    Active: [RUNNING] since 2021-09-26 19:27:39; 52:35 ago
  Main PID: 326401 (curvefs_mds)
    Daemon: True
    Listen: 0.0.0.0:17700
       Mem: 55520 KB
```

[Back to Toc](#table-of-contents)

[bazel_install]: https://docs.bazel.build/versions/main/install.html
[ansible_install]: https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html#installing-ansible-on-specific-operating-systems
[fuse_install]: https://github.com/libfuse/libfuse#installation
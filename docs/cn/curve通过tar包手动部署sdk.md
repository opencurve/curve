## 通过tar包的方式手动安装curve sdk


### 1. 下载curve的tar包

在curve的[releases](https://github.com/opencurve/curve/releases)页面，下载需要的版本。以v1.2.5为例，下载tar包。

`wget https://github.com/opencurve/curve/releases/download/untagged-5d1b741a770ebfee3ad2/curve_1.2.5+2c4861ca.tar.gz`

### 2. 解压tar包

tar -zxvf curve_1.2.5+2c4861ca.tar.gz

### 3. 安装依赖的包

如果是Debian/Ubuntu系统

`sudo apt-get -y install libunwind8`

`sudo apt-get -y install libunwind8-dev`

如果是Redhat/CentOs系统

`sudo yum -y install libunwind`

`sudo yum -y install libunwind-devel`

### 4. 拷贝所需的文件到目标目录

`cd curve/curve-sdk`

拷贝lib，如果是Debian/Ubuntu系统

`cp lib/* /usr/lib`

拷贝lib，如果是Redhat/CentOs系统

`cp lib/* /usr/lib64`

拷贝剩下的目录到指定的位置

`cp bin/* /usr/bin`

`cp curvefs/* /usr/curvefs`

`cp include/* /usr/include`




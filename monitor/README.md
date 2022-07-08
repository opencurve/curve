## 目录结构介绍

##### curve-monitor.sh

curve集群监控的控制脚本，用于启动、停止、重启监控功能。

##### docker-compose.yml:

编排监控系统相关容器的配置文件，包括prometheus容器、grafana容器。

修改该文件来配置各组件的配置参数。

##### update_dashboard.sh:

从grafana界面配置环境当中拉取最新的dashboard，用于更新该环境上grafana的界面。

##### target_json.py

用于生成prometheus监控对象的python脚本，每隔一段时间用curve_ops_tool拉取监控目标并更新。

##### target.ini

target_json.py脚本依赖的一些配置

##### grafana:

grafana相关目录

##### grafana/grafana.ini:

grafana的启动配置文件，将映射到容器的 `/etc/grafana/grafana.ini` 上

##### grafana/report:
grafana日报临时目录，将映射到reporter容器的`/tmp/report`目录上

##### grafana/provisioning:

grafana预配置相关目录，将映射到容器的`/etc/grafana/provisioning`上

##### grafana/provisioning/dashboards:

grafana所有dashboards的json文件存放目录，grafana将从该目录加载文件来创建dashboards；通过update_dashboard.sh脚本来更新最新的dashboards。

##### grafana/provisioning/datasources:

grafana的datasources的json文件存放目录，grafana将从该目录加载文件来创建datasources。

##### prometheus:

prometheus相关目录

##### prometheus/prometheus.yml:

prometheus的配置文件



## 使用说明

#### 环境初始化

1.部署监控系统的机器需要安装如下组件：

docker、docker-compose、jq

* docker安装

```
$ curl -fsSL get.docker.com -o get-docker.sh
$ sudo sh get-docker.sh --mirror Aliyun
```

或者直接安装

```
apt-get install docker-ce
apt-get install docker-ce-cli
```

* docker-compose

* ```
  curl -L https://github.com/docker/compose/releases/download/1.18.0/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
  chmod +x /usr/local/bin/docker-compose
  ```

  或者直接安装

  ```
  apt-get install docker-compose
  ```

* jq

  update_dashboard.sh脚本需要依赖jq命令，这个一般机器上都没装

  ```
  apt-get install jq
  ```

#### 部署监控系统

* 修改相关配置

1.修改target_json.py文件中相应的配置

2.修改update_dashboard.sh，将 URL 和 LOGIN 改为对应的地址和用户名密码

3.修改docker-compose.yml文件，主要是映射的目录路径

* 启动docker-compose

在当前目录下执行如下命令即可

```curve-monitor.sh start ```

* 部署grafana每日报表

crontab配置定时任务，添加如下任务：
30 8 * * * python /etc/curve/monitor/report.py >> /etc/curve/monitor/cron.log 2>&1
如果机器上没有配置其他的定时任务，可直接用下面命令
echo "30 8 * * * python /etc/curve/monitor/report.py >> /etc/curve/monitor/cron.log 2>&1" >> conf && crontab conf && rm -f conf


## 目录结构介绍

##### curve-monitor.sh

curve集群监控的控制脚本，用于启动、停止、重启监控功能。

##### docker-compose.yml.example:

编排监控系统相关容器的配置文件，包括prometheus容器、grafana容器、mysql_exporter容器。

修改该文件来配置各组件的配置参数。

##### update_dashboard.sh:

从grafana界面配置环境当中拉取最新的dashboard，用于更新该环境上grafana的界面。

##### target_json.py

用于生成prometheus监控对象的python脚本，每隔一段时间会去从mysql数据库里拉取监控目标并更新。

##### target.ini.example

target_json.py脚本依赖的一些配置

##### grafana:

grafana相关目录

##### grafana/grafana.ini.example:

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

##### prometheus/prometheus.yml.example:

prometheus的配置文件



## 使用说明

以下步骤为不使用puppet进行部署的过程。

#### 环境初始化

1.部署监控系统的机器需要安装如下组件：

node_exporter、docker、docker-compose、python-mysqldb、jq

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

* node_exporter

  可能很多节点都要安装，可以用脚本来一起装，如下面的方式：

  ```
  for i in {1..4};
  do
  scp -P 1046 ~/Downloads/node_exporter-0.18.1.linux-amd64.tar.gz yangyaokai@pubt1-curve$i.yq.163.org:~/
  ssh -p 1046 yangyaokai@pubt1-curve$i.yq.163.org "tar zxvf node_exporter-0.18.1.linux-amd64.tar.gz ; cd node_exporter-0.18.1.linux-amd64 ; nohup ./node_exporter >/dev/null 2>log &"
  echo $i
  done
  ```

* python-mysqldb

  python脚本操作mysql需要依赖这个库

  ```apt-get install python-mysqldb```

* jq

  update_dashboard.sh脚本需要依赖jq命令，这个一般机器上都没装

  ```
  apt-get install jq
  ```

2.chunkserver上安装node_exporter（机器监控可以依赖哨兵，可以不装）

3.mysql需要添加监控用户（也可以直接使用使用账户）

```
mysql -uroot -pqwer
CREATE USER 'exporter'@'%' IDENTIFIED BY 'asdf' WITH MAX_USER_CONNECTIONS 3;
GRANT PROCESS, REPLICATION CLIENT, SELECT ON *.* TO 'exporter'@'%';
flush privileges;
```



#### 部署监控系统

* 修改相关配置

1.修改target_json.py文件中相应的配置

2.修改update_dashboard.sh，将 URL 和 LOGIN 改为对应的地址和用户名密码

3.修改docker-compose.yml文件，主要是映射的目录路径以及mysql的ip和用户密码

* 启动docker-compose

在当前目录下执行如下命令即可

```curve-monitor.sh start ```

* 部署grafana每日报表

crontab配置定时任务，添加如下任务：
30 8 * * * python /etc/curve/monitor/report.py >> /etc/curve/monitor/cron.log 2>&1
如果机器上没有配置其他的定时任务，可直接用下面命令
echo "30 8 * * * python /etc/curve/monitor/report.py >> /etc/curve/monitor/cron.log 2>&1" >> conf && crontab conf && rm -f conf




#### 对接puppet

如果对接puppet，配置相关文件都会放到puppet上，配置的变更都要上传到puppet上。

puppet上管理的配置包括：docker-compose.yml、target.ini、grafana.ini、prometheus.yml

通过安装包安装完curve-monitor以后，会将curve-monitor.sh拷贝到/usr/bin目录下，可以通过以下命令管理监控系统：

启动：```curve-monitor.sh start```

停止：```curve-monitor.sh stop```

重启：```curve-monitor.sh restart```

上面环境初始化中的依赖的包puppet基本都会帮忙安装，除了node_exporter需要自己安装。

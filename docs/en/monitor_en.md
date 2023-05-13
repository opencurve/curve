[中文版](../cn/monitor.md)

# Monitoring System

## Overview

CURVE monitoring system includes three aspects: indicators collection, indicators storage, and indicators display. For indicators collection, use [brpc](https://github.com/apache/incubator-brpc) built-in [bvar](https://github.com/apache/incubator-brpc/blob/master/docs/en/bvar.md); the open source monitoring system [prometheus](https://prometheus.io/docs/introduction/overview/) is used for indicators storage; [grafana](https://github.com/grafana/grafana) is used for indicators display.

## bvar

[bvar](https://github.com/apache/incubator-brpc/blob/master/docs/en/bvar.md) is a counter library in a multi-threaded environment, which is convenient for recording and viewing various types of user numerical value in program. bvar data can be exported and queried via web portal on the port of the brpc server service, also can view historical trends, statistics and quantile values; bvar also has a built-in prometheus conversion module to convert collected indicators into a format supported by prometheus.

The bvar data models used in CURVE are:

`bvar::Adder<T>` : counter，default 0，varname << N is equivalent to varname += N.

`bvar::Maxer<T>` : maximum value，default std::numeric_limits::min()，varname << N is equivalent to varname = max(varname, N).

`bvar::Miner<T>` : minimum value，default std::numeric_limits::max()，varname << N is equivalent to varname = min(varname, N).

`bvar::IntRecorder` : the average value since use. Note that the attributive here is not "in a period of time". Generally, the average value in the time window is derived through Window.

`bvar::Window<VAR>`: the accumulated value of a bvar in a period of time. Window is derived from the existing bvar and will be updated automatically.

`bvar::PerSecond<VAR>`: the average accumulated value of a bvar per second in a period of time. PerSecond is also a derivative variable that is automatically updated.

`bvar::LatencyRecorder`: to recording latency and qps. Input delay, average delay/maximum delay/qps/total times are all available.

The specific usage of bvar in CURVE can be viewed:

[client metric](../../src/client/client_metric.h) 

[chunkserver metric](../../src/chunkserver/chunkserver_metrics.h)   

[mds topology metric](../../src/mds/topology/topology_metric.h)

[mds schedule metric](../../src/mds/schedule/scheduleMetrics.h)

## prometheus + grafana

CURVE cluster monitoring uses Prometheus to collect data and Grafana to display.

Monitoring content includes: Client, Mds, Chunkserver, Etcd, and machine nodes.

The configuration of the monitoring target uses the prometheus file-based service automatic discovery function. The monitoring component is deployed in docker and docker-compose is used for orchestration. The deployment related scripts are in the [CURVE warehouse](../../monitor).

<img src="../images/monitor.png" alt="monitor" width="800" />

1. ```Promethethus``` regularly pulls corresponding data from Brpc Server in MDS, ETCD, Snapshotcloneserver, ChunkServer, and Client.

2. ```docker compose``` is used to orchestrate the configuration of docker components, including Prometheus, Grafana and Repoter.

3. ```python``` scripts. [target_json.py](../../monitor/target_json.py) is used to generate the monitoring target configuration that prometheus service discovery depends on. The generated file is in json format and the script depends on [target.ini](../../monitor/target.ini) to obtain mds, etcd information from the configuration. [grafana-report.py](../../monitor/grafana-report.py) is used to export the data information required by the daily reporter from Grafana.

## Renderings Show

##### Grafana renderings

<img src="../images/grafana-example-1.png" alt="monitor" width="1000" />

<img src="../images/grafana-example-3.png" alt="monitor" width="1000" />

<img src="../images/grafana-example-2.png" alt="monitor" width="1000" />

##### Daily Report renderings

<img src="../images/grafana-reporter.png" alt="monitor" width="800" />


# Curve块存储监控项说明

Curve块存储使用grafana+promethues的方式实现监控可视化，grafan面板上的监控项含义如下。

**MDS**

<table>
    <tr>
        <td>DashBord</td>
        <td>Mean</td>
        <td>Pannel</td>
    </tr>
    <tr>
        <td rowspan="3">进程资源</td>
        <td rowspan="3">进程的运行时间，对cpu的占用，对内存的占用</td>
        <td>进程运行时间</td>
    </tr>
    <tr>
        <td>进程cpu使用情况</td>
    </tr>
    <tr>
        <td>内存占用</td>
    </tr>
    <tr>
        <td rowspan="1">逻辑池状态</td>
        <td rowspan="1">记录了逻辑池空间分配的一些情况</td>
        <td>logical_pool_metrics</td>
    </tr>
    <tr>
        <td rowspan="8">chunkserver状态</td>
        <td rowspan="8">chunkserver相关的统计信息。其中_bps指的单个chunkserver上每秒带宽；_iops指的单个chunkserver上每秒请求数量；这两个数据是chunkserver上报给mds的，是mds进行调度的依据之一</td>
        <td>chunkserver_leader_num_series 每个chunkserver上leader的数量,可以看到随时间的变化趋势</td>
    </tr>
    <tr>
        <td>chunkserver_current_leader_count 每个chunkserver上leader的数量，用于判断leader在集群中的分布是否均衡</td>
    </tr>
    <tr>
        <td>chunkserver_current_copyset_num 每个chunkserver上copyset的数量</td>
    </tr>
    <tr>
        <td>chunkserver_current_scatterwidth 每个chunkserver的scatterwidth值，用于判断copyset在集群中的分布集中度</td>
    </tr>
    <tr>
        <td>chunkserver_current_write_bps</td>
    </tr>
    <tr>
        <td>chunkserver_current_write_iops</td>
    </tr>
    <tr>
        <td>chunkserver_current_read_bps</td>
    </tr>
    <tr>
        <td>chunkserver_current_read_iops</td>
    </tr>
    <tr>
        <td rowspan="6">调度监控</td>
        <td rowspan="6">用于统计当前集群中配置变更的情况</td>
        <td>operator num 当前进行中和等待执行的配置变更任务个数</td>
    </tr>
    <tr>
        <td>add_peer_num 复制组新增一个peer的任务个数</td>
    </tr>
    <tr>
        <td>transfer_leader_num 复制组leader变更的任务个数</td>
    </tr>
    <tr>
        <td>remove_peer_num 复制组减少一个peer的任务个数</td>
    </tr>
    <tr>
        <td>remove_peer_num 普通优先级任务个数(remove_peer/transfer_leader)</td>
    </tr>
    <tr>
        <td>remove_peer_num 高先级任务个数(add_peer)</td>
    </tr>
    <tr>
        <td rowspan="4">RPC层指标</td>
        <td rowspan="4">用于统计mds接受到的请求情况。其中_qps指每秒接受到的请求个数；_concurrency指接受请求的并发度；_lat指处理请求的延迟</td>
        <td>total_rpc_qps</td>
    </tr>
    <tr>
        <td>total_rpc_concurrency</td>
    </tr>
    <tr>
        <td>request_rpc_qps</td>
    </tr>
    <tr>
        <td>request_rpc_lat</td>
    </tr>
    <tr>
        <td rowspan="5">HeartBeat指标</td>
        <td rowspan="5">用于统计chunkserver和mds之间心跳请求的情况。其中_qps/_concurrency和RPC层指标类似；lat_avg指处理请求的平均时延；_lat_percentile指的是请求的百分位点延迟(50/80/90/99分位点)</td>
        <td>total_rpc_qps</td>
    </tr>
    <tr>
        <td>heartbeat_qps</td>
    </tr>
    <tr>
        <td>heartbeat_concurrency</td>
    </tr>
    <tr>
        <td>heartbeat_lat_avg</td>
    </tr>
    <tr>
        <td>heartbeat_lat_percentile</td>
    </tr>
</table>


**ChunkServer**
<table>
    <tr>
        <td>DashBord</td>
        <td>Mean</td>
        <td>Pannel</td>
    </tr>
    <tr>
        <td rowspan="2">进程资源占用</td>
        <td rowspan="2">进程的运行时间，对cpu的占用</td>
        <td>进程运行时间</td>
    </tr>
    <tr>
        <td>porcess_cpu_usage</td>
    </tr>
    <tr>
        <td rowspan="12">rpc层读写指标</td>
        <td rowspan="12">这是brpc对service的一些统计指标，rpc层的请求包括了rpc接受-数据处理-数据发送三个阶段。其中_qps指的是每秒处理的请求数量；_eps指的是每秒请求处理返回error的数量；_concurrency指的是并发度；_lat指的是请求延迟；_lat_percentile指的是请求的百分位点延迟(50/80/90/99分位点)</td>
        <td>total_rpc_qps</td>
    </tr>
    <tr>
        <td>iobuf_memory</td>
    </tr>
        <td>read_chunk_eps</td>
    </tr>
    <tr>
        <td>read_chunk_concurrency</td>
    </tr>
    <tr>
        <td>read_chunk_qps</td>
    </tr>
    <tr>
        <td>write_chunk_eps</td>
    </tr>
    <tr>
        <td>write_chunk_concurrency</td>
    </tr>
    <tr>
        <td>write_chunk_qps</td>
    </tr>
    <tr>
        <td>avg_read_lat</td>
    </tr>
    <tr>
        <td>read_chunk_lat_percentile</td>
    </tr>
    <tr>
        <td>avg_write_lat</td>
    </tr>
    <tr>
        <td>write_chunk_lat_percentile</td>
    </tr>
    <tr>
        <td rowspan="14">chunkserver层读写指标</td>
        <td rowspan="14">这是chunkserver的处理时间，和rpc层读写指标最大的区别是不包括rpc的接受和发出两个阶段。其中_eps指的是每秒请求处理返回error的数量；_qps指的是每秒处理请求数量；_rps指的是每秒接受到的请求数量，rps-qps就是当前正在等待处理的请求数量；_lat指的是请求延迟；_lat_percentile指的是请求的百分位点延迟(50/80/90/99分位点)；特别要说明的是*_io_size_percentile指的是读写请求的IO大小</td>
        <td>read_chunk_eps</td>
    </tr>
    <tr>
        <td>read_chunk_iops</td>
    </tr>
    <tr>
        <td>read_chunk_rps</td>
    </tr>
    <tr>
        <td>write_chunk_eps</td>
    </tr>
    <tr>
        <td>write_chunk_iops</td>
    </tr>
    <tr>
        <td>write_chunk_rps</td>
    </tr>
    <tr>
        <td>read_io_latency_percentile</td>
    </tr>
    <tr>
        <td>write_io_latency_percentile</td>
    </tr>
    <tr>
        <td>avg_read_lat</td>
    </tr>
    <tr>
        <td>avg_write_lat</td>
    </tr>
    <tr>
        <td>read_bps</td>
    </tr>
    <tr>
        <td>write_bps</td>
    </tr>
    <tr>
        <td>read_io_size_percentile</td>
    </tr>
    <tr>
        <td>write_io_size_percentile</td>
    </tr>
    <tr>
        <td rowspan="13">copyset指标</td>
        <td rowspan="13">主要指的是单个copyset对读写请求的处理统计指标。其中_eps/_iops/_rps跟chunkserver层读写指标或者rpc层读写指标含义相同；_bps指的是每秒处理IO的带宽</td>
        <td>selected copyset count</td>
    </tr>
    <tr>
        <td>selected copyset chunk sum</td>
    </tr>
    <tr>
        <td>copyset_chunk_num</td>
    </tr>
    <tr>
        <td>copyset_read_chunk_eps</td>
    </tr>
    <tr>
        <td>copyset_read_chunk_iops</td>
    </tr>
    <tr>
        <td>copyset_read_chunk_rps</td>
    </tr>
    <tr>
        <td>copyset_write_chunk_eps</td>
    </tr>
    <tr>
        <td>copyset_write_chunk_iops</td>
    </tr>
    <tr>
        <td>copyset_write_chunk_rps</td>
    </tr>
    <tr>
        <td>copyset_read_chunk_bps</td>
    </tr>
    <tr>
        <td>copyset_write_chunk_bps</td>
    </tr>
    <tr>
        <td>copyset_avg_read_lat</td>
    </tr>
    <tr>
        <td>copyset_avg_write_lat</td>
    </tr>
    <tr>
        <td rowspan="3">chunkserver关键指标</td>
        <td rowspan="3">chunkserver上关于容量的一些统计数据</td>
        <td>total_chunk_num 文件池中所有的(已经使用和未使用的)chunk的数量</td>
    </tr>
    <tr>
        <td>chunkfilepool_left_chunks 文件池中剩余的chunk个数</td>
    </tr>
    <tr>
        <td>copyset_count copyset的数量</td>
    </tr>
    <tr>
        <td rowspan="12">Raft关键指标</td>
        <td rowspan="12">数据复制过程中关键步骤的指标</td>
        <td>raft_num_log_entries 复制组内存中日志的数量</td>
    </tr>
    <tr>
        <td>raft_apply_tasks_batch_counter 复制组每次apply任务的数量</td>
    </tr>
    <tr>
        <td>raft_send_entries_latency 复制组leader发送数据给follower+follower收到数据进行日志持久化+follower回复复制成功这三个阶段的时间总和</td>
    </tr>
    <tr>
        <td>raft_send_entries_qps 复制组leader发送数据给follower的每秒请求数量</td>
    </tr>
    <tr>
        <td>raft_service_append_entries_latency 复制组中follower处理请求的时延</td>
    </tr>
    <tr>
        <td>raft_service_append_entries_qps 复制组中follower每秒接受请求的数量</td>
    </tr>
    <tr>
        <td>raft_install_snapshot_qps 复制组的副本每秒接受到安装快照请求的数量</td>
    </tr>
    <tr>
        <td>storage_append_entries_latency 复制组中的副本日志写盘的延迟</td>
    </tr>
    <tr>
        <td>install_snapshot_bps 复制组的副本执行安装快照时的带宽</td>
    </tr>
    <tr>
        <td>raft_get_file_qps 复制组每秒处理获取文件请求的个数</td>
    </tr>
    <tr>
        <td>leader_need_install_snapshot_task_num 复制组中leader处理快照任务的并发度（一个copyset上可能有多个leader在进行）</td>
    </tr>
    <tr>
        <td>follower_install_snapshot_task_num 复制组中follower正在下载快照的任务数量（一个copyset最多一个任务）</td>
    </tr>
    <tr>
        <td rowspan="4">线程指标</td>
        <td rowspan="4">brpc中线程相关的一些指标，一定程度上可以反应当前服务的压力情况</td>
        <td>bthread_worker_count chunkserver服务brpc组件中配置的线程数量/td>
    </tr>
    <tr>
        <td>bthread_worker_usage chunkserver服务brpc组件中线程的实际使用数量</td>
    </tr>
    <tr>
        <td>bthread_count chunkserver服务brpc组件中协程的数量</td>
    </tr>
    <tr>
        <td>bthread_execq chunkserver服务brpc组件中可执行队列的数量</td>
    </tr>
</table>

**Client**

<table>
    <tr>
        <td>DashBord</td>
        <td>Mean</td>
        <td>Pannel</td>
    </tr>
    <tr>
        <td rowspan="2">进程资源</td>
        <td rowspan="2">进程的运行时间，对cpu的占用</td>
        <td>客户端运行时间</td>
    </tr>
    <tr>
        <td>client_cpu_usage</td>
    </tr>
    <tr>
        <td rowspan="1">客户端配置</td>
        <td rowspan="1">客户端的所有配置项</td>
        <td>客户端配置</td>
    </tr>
    <tr>
        <td rowspan="9">用户接口层指标</td>
        <td rowspan="9">从客户端向外提供的接口相关的性能统计数据。其中_qps指的是每秒处理请求数量；_rps指的是每秒接受到的请求数量，rps-qps就是当前正在等待处理的请求数量；_latency指的是请求延迟；_bps指的是每秒处理IO的带宽；特别要说明的是*_io_size_percentile指的是请求的IO大小</td>
        <td>client_read_qps</td>
    </tr>
    <tr>
        <td>client_write_qps</td>
    </tr>
    <tr>
        <td>client_read_rps</td>
    </tr>
    <tr>
        <td>client_write_rps</td>
    </tr>
    <tr>
        <td>client_read_bps</td>
    </tr>
    <tr>
        <td>client_write_bps</td>
    </tr>
    <tr>
        <td>client_read_latency</td>
    </tr>
    <tr>
        <td>client_write_latency</td>
    </tr>
    <tr>
        <td>write_io_size_percentile</td>
    </tr>
    <tr>
        <td rowspan="2">中间业务层指标</td>
        <td rowspan="2">IO过程中的两个重要指标项</td>
        <td>inflight_rpc_num 正在处理中的IO请求的个数</td>
    </tr>
    <tr>
        <td>get_leader_retry_rpc 从chunkserver获取复制组当前leader的重试rpc的次数</td>
    </tr>
    <tr>
        <td rowspan="7">rpc层指标</td>
        <td rowspan="7">client和chunkserver进行IO交互过程中的指标，主要包括client发送请求+chunkserver处理请求+client收到请求三个阶段的时间。其中_qps/_bps/_latency和用户接口层指标中的含义类似</td>
        <td>read_rpc_qps</td>
    </tr>
    <tr>
        <td>write_rpc_qps</td>
    </tr>
    <tr>
        <td>read_rpc_bps</td>
    </tr>
    <tr>
        <td>write_rpc_qps</td>
    </tr>
    <tr>
        <td>read_rpc_latency</td>
    </tr>
    <tr>
        <td>write_rpc_latency</td>
    </tr>
    <tr>
        <td>timeout_qps</td>
    </tr>
    <tr>
        <td rowspan="1">与MDS通信指标</td>
        <td rowspan="1">client和mds所有rpc每秒请求次数统计</td>
        <td>client2mds_rpc_qps</td>
    </tr>
</table>


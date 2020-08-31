<head><meta charset="UTF-8"></head>

## curve_ops_tool 工具使用说明

Usage: curve_ops_tool [Command] [OPTIONS...]

查看使用案例 : curve_ops_tool space --example
   
Commands:
   
      space: 展示curve磁盘所有类型的空间，包括总的空间和已使用空间。
      status: 展示集群的所有状态信息。
      chunkserver-status: 展示chunkserver在线状态信息。
      mds-status: 展示mds状态信息。
      client-status: 展示客户端状态信息。
      etcd-status: 展示etcd状态信息。
      snapshot-clone-status: 展示快照克隆服务器状态。
      copysets-status: 检查所有copyset的健康状态。
      chunkserver-list: 展示chunkserver列表和chunkserver信息。
      get: 展示文件信息和文件的真实空间。
      list: 列出目录下所有文件的文件信息。
      seginfo: 展示文件的segment信息。
      delete: 删除文件，强制删除使用 --forcedelete=true。
      clean-recycle: 清空RecycleBin。
      create: 创建文件，文件长度以GB为单位。
      chunk-location: 查询相应偏移的chunk的位置信息。
      check-consistency: 检查三副本的一致性。
      remove-peer: 从copyset中移除节点。
      transfer-leader: 转换copyset上的leader角色给一个节点。
      reset-peer: 重置copyset配置，仅支持针对一个节点的重置。
      check-chunkserver: 检查一个chunkserver的健康状态。
      check-copyset: 检查一个copyset的健康状态。
      check-server: 检查一个server的健康状态。
      check-operator: 检查操作者。
      rapid-leader-schedule: 逻辑池中集群的快速leader调度。
	  
     可以在配置文件中进行配置，避免携带太多可选项，工具默认配置文件在 /etc/curve/tools.conf，自定义路径可以通过 -confPath 来指定配置文件。
    注意：显式指定的参数和-confPath指定的配置文件同时存在时，显式指定参数会覆盖配置文件参数。
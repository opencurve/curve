[中文版](../cn/curve_ops_tool.md)

<head><meta charset="UTF-8"></head>

## curve_ops_tool help

Usage: curve_ops_tool [Command] [OPTIONS...]

show command example : curve_ops_tool space --example
   
Commands:

     space : show curve all disk type space, include total space and used space
     status : show the total status of the cluster
     cluster-status : show cluster status
     chunkserver-status : show the chunkserver online status
     mds-status : show the mds status
     client-status : show the client status
     client-list : list all client
     etcd-status : show the etcd status
     snapshot-clone-status : show the snapshot clone server status
     copysets-status : check the health state of all copysets
     chunkserver-list : show curve chunkserver-list, list all chunkserver information
     server-list : list all server information
     logical-pool-list : list all logical pool information
     get : show the file info and the actual space of file
     list : list the file info of files in the directory
     seginfo : list the segments info of the file
     delete : delete the file, to force delete, should specify the --forcedelete=true
     clean-recycle : clean the RecycleBin
     create : create file, file length unit is GB
     chunk-location : query the location of the chunk corresponding to the offset
     check-consistency : check the consistency of three copies
     remove-peer : remove the peer from the copyset
     transfer-leader : transfer the leader of the copyset to the peer
     reset-peer : reset the configuration of copyset, only reset to one peer is supported
     do-snapshot : do snapshot of the peer of the copyset
     do-snapshot-all : do snapshot of all peers of all copysets
     check-chunkserver : check the health state of the chunkserver
     check-copyset : check the health state of one copyset
     check-server : check the health state of the server
     check-operator : check the operators
     list-may-broken-vol: list all volumes on majority offline copysets
     rapid-leader-schedule: rapid leader schedule in cluster in logicalpool

      It can be configured in the configuration file to avoid carrying too many options. The default configuration file of the tool is /etc/curve/tools.conf, and the custom path can be specified by -confPath.
     Note: When the explicitly specified parameters and the configuration file specified by -confPath exist at the same time, the explicitly specified parameters will overwrite the configuration file parameters.

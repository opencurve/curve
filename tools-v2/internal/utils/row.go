/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: CurveCli
 * Created Date: 2022-05-25
 * Author: chengyi (Cyber-SiKu)
 */

package cobrautil

const (
	ROW_ROLE                 = "role"
	ROW_ADDR                 = "addr"
	ROW_ALLOC                = "alloc"
	ROW_ALLOC_SIZE           = "allocatedSize"
	ROW_BLOCKSIZE            = "blocksize"
	ROW_CAPACITY             = "capacity"
	ROW_CHILD_LIST           = "childList"
	ROW_CHILD_TYPE           = "childType"
	ROW_CHUNK                = "chunk"
	ROW_CHUNK_SIZE           = "chunkSize"
	ROW_COPYSET              = "copyset"
	ROW_COPYSET_ID           = "copysetId"
	ROW_COPYSET_KEY          = "copysetKey"
	ROW_CREATE_TIME          = "createTime"
	ROW_CREATED              = "created"
	ROW_CTIME                = "ctime"
	ROW_DUMMY_ADDR           = "dummyAddr"
	ROW_END                  = "end"
	ROW_EPOCH                = "epoch"
	ROW_EXPLAIN              = "explain"
	ROW_EXTERNAL_ADDR        = "externalAddr"
	ROW_FILE_NAME            = "fileName"
	ROW_FILE_SIZE            = "fileSize"
	ROW_FILE_TYPE            = "fileType"
	ROW_FS_ID                = "fsId"
	ROW_FS_NAME              = "fsName"
	ROW_FS_TYPE              = "fsType"
	ROW_GROUP                = "group"
	ROW_HOSTNAME             = "hostname"
	ROW_ID                   = "id"
	ROW_INODE_ID             = "inodeId"
	ROW_INTERNAL_ADDR        = "internalAddr"
	ROW_IP                   = "ip"
	ROW_KEY                  = "key"
	ROW_LEADER               = "leader"
	ROW_FOLLOWER             = "follower"
	ROW_OLDLEADER            = "oldLeader"
	ROW_LEADER_PEER          = "leaderPeer"
	ROW_LEFT                 = "left"
	ROW_LENGTH               = "length"
	ROW_LOCATION             = "location"
	ROW_LOG_GAP              = "logGap"
	ROW_LOGICALPOOL          = "logicalpool"
	ROW_METASERVER           = "metaserver"
	ROW_METASERVER_ADDR      = "metaserverAddr"
	ROW_MOUNT_NUM            = "mountNum"
	ROW_MOUNTPOINT           = "mountpoint"
	ROW_NAME                 = "name"
	ROW_NLINK                = "nlink"
	ROW_NUM                  = "num"
	ROW_ONLINE_STATE         = "onlineState"
	ROW_OPERATION            = "operation"
	ROW_OPNAME               = "opname"
	ROW_ORIGINAL_PATH        = "originalPath"
	ROW_OWNER                = "owner"
	ROW_PARENT               = "parent"
	ROW_PARENT_ID            = "parentId"
	ROW_PARTITION_ID         = "partitionId"
	ROW_PEER                 = "peer"
	ROW_PEER_ADDR            = "peerAddr"
	ROW_PEER_ID              = "peerId"
	ROW_PEER_NUMBER          = "peerNumber"
	ROW_PHYPOOL              = "phyPool"
	ROW_POOL                 = "pool"
	ROW_POOL_ID              = "poolId"
	ROW_PORT                 = "port"
	ROW_READONLY             = "readonly"
	ROW_REASON               = "reason"
	ROW_RECOVERING           = "recovering"
	ROW_RECYCLABLE           = "recyclable"
	ROW_RECYCLE              = "recycle"
	ROW_RESULT               = "result"
	ROW_SCAN                 = "scan"
	ROW_LASTSCAN             = "last Scan"
	ROW_LAST_SCAN_CONSISTENT = "last Scan Consistent"
	ROW_SEGMENT              = "segment"
	ROW_SEGMENT_SIZE         = "segmentSize"
	ROW_SEQ                  = "seq"
	ROW_SERVER               = "server"
	ROW_SIZE                 = "size"
	ROW_START                = "start"
	ROW_STATE                = "state"
	ROW_STATUS               = "status"
	ROW_HEALTHY              = "healthy"
	ROW_UNHEALTHY            = "unhealthy"
	ROW_STRIPE               = "stripe"
	ROW_SUM_IN_DIR           = "sumInDir"
	ROW_TERM                 = "term"
	ROW_THROTTLE             = "throttle"
	ROW_TOTAL                = "total"
	ROW_TYPE                 = "type"
	ROW_USED                 = "used"
	ROW_VERSION              = "version"
	ROW_ZONE                 = "zone"
	ROW_AVAILFLAG            = "availFlag"
	ROW_DRYRUN               = "dryrun"
	ROW_HEALTHY_COUNT        = "healthyCount"
	ROW_UNHEALTHY_COUNT      = "unhealthyCount"
	ROW_HEALTHY_RATIO        = "ratio"
	ROW_UNHEALTHY_RATIO      = "unhealthy-Ratio"

	ROW_RW_STATUS         = "rwStatus"
	ROW_DISK_STATE        = "diskState"
	ROW_COPYSET_NUM       = "copysetNum"
	ROW_DISK_CAPACITY     = "diskCapacity"
	ROW_DISK_USED         = "diskUsed"
	ROW_UNHEALTHY_COPYSET = "unhealthyCopyset"
	ROW_EXT_ADDR          = "extAddr"

	// s3
	ROW_S3CHUNKINFO_CHUNKID = "s3ChunkId"
	ROW_S3CHUNKINFO_LENGTH  = "s3Length"
	ROW_S3CHUNKINFO_OFFSET  = "s3Offset"
	ROW_S3CHUNKINFO_SIZE    = "s3Size"

	// value
	ROW_VALUE_ADD            = "add"
	ROW_VALUE_DEL            = "del"
	ROW_VALUE_DNE            = "DNE"
	ROW_VALUE_FAILED         = "failed"
	ROW_VALUE_LOGICAL        = "logical"
	ROW_VALUE_NO_RECOVERING  = ""
	ROW_VALUE_NO_VALUE       = "-"
	ROW_VALUE_NULL           = "null"
	ROW_VALUE_OFFLINE        = "offline"
	ROW_VALUE_ONLINE         = "online"
	ROW_VALUE_PHYSICAL       = "physical"
	ROW_VALUE_RECOVERING_OUT = "recovering from out"
	ROW_VALUE_SUCCESS        = "success"
	ROW_VALUE_UNKNOWN        = "unknown"
	ROW_VALUE_TRUE           = "true"
	ROW_VALUE_FALSE          = "false"
)

// topology type
const (
	TYPE_POOL    = "pool"
	TYPE_SERVER  = "server"
	TYPE_ZONE    = "zone"
	TYPE_PHYPOOL = "physicalpool"
	TYPE_LGPOOL  = "logicalpool"
)

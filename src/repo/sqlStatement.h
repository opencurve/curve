/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * Created Date: Wed Sep 19 2018
 * Author: lixiaocui
 */


#ifndef SRC_REPO_SQLSTATEMENT_H_
#define SRC_REPO_SQLSTATEMENT_H_

#include <string>
#include <map>
#include <cstring>

namespace curve {
namespace repo {

const char ChunkServerTable[] = "curve_chunkserver";
const char ServerTable[] = "curve_server";
const char ZoneTable[] = "curve_zone";
const char LogicalPoolTable[] = "curve_logicalpool";
const char PhysicalPoolTable[] = "curve_physicalpool";
const char CopySetTable[] = "curve_copyset";
const char SessionTable[] = "curve_session";
const char SnapshotTable[] = "snapshot";
const char CloneTable[] = "clone";
const char ClientInfoTable[] = "client_info";
const char ClusterInfoTable[] = "curve_clusterInfo";

const char CreateClusterInfoTable[] =
    "create table if not exists `curve_clusterInfo` (\n"  //NOLINT
    "    `clusterId`        varchar(128)    NOT NULL PRIMARY KEY COMMENT 'clusterId'\n"  //NOLINT
    ")COMMENT='clusterInfo';";  //NOLINT

const char CreateChunkServerTable[] =
    "create table if not exists `curve_chunkserver` (\n"
    "    `chunkServerID`     int            NOT NULL PRIMARY KEY COMMENT 'chunk server id',\n"      //NOLINT
    "    `token`             varchar(16)    NOT NULL COMMENT 'token to identity chunk server',\n"   //NOLINT
    "    `diskType`          varchar(8)     NOT NULL COMMENT 'disk type',\n"
    "    `internalHostIP`    varchar(16)    NOT NULL COMMENT 'internal ip',\n"
    "    `port`              int            NOT NULL COMMENT 'port',\n"
    "    `rwstatus`          tinyint        NOT NULL COMMENT 'chunk server status: rw/ro/wo/pending/retired',\n"    //NOLINT
    "    `serverID`          int            NOT NULL COMMENT 'server where chunk server in',\n"         //NOLINT
    "    `onlineState`       tinyint        NOT NULL COMMENT 'chunk server state: online/offline',\n"   //NOLINT
    "    `diskState`         tinyint        NOT NULL COMMENT 'disk state: DiskError, DiskNormal',\n"    //NOLINT
    "    `mountPoint`        varchar(32)    NOT NULL COMMENT 'disk mount point, e.g /mnt/ssd1',\n"      //NOLINT
    "    `capacity`          bigint         NOT NULL COMMENT 'total size of disk',\n"                   //NOLINT
    "    `used`              bigint         NOT NULL COMMENT 'used space'\n"
    ")COMMENT='chunk server';";

const char CreateServerTable[] =
    "create table if not exists `curve_server` (\n"
    "    `serverID`          int           NOT NULL PRIMARY KEY COMMENT 'server id',\n" //NOLINT
    "    `hostName`          varchar(32)   NOT NULL COMMENT 'host name',\n"
    "    `internalHostIP`    varchar(16)   NOT NULL COMMENT 'internal host ip',\n"      //NOLINT
    "    `internalPort`      int           NOT NULL COMMENT 'internal port',\n"
    "    `externalHostIP`    varchar(16)   NOT NULL COMMENT 'external host ip',\n"      //NOLINT
    "    `externalPort`      int           NOT NULL COMMENT 'external port',\n"
    "    `zoneID`            int           NOT NULL COMMENT 'zone id it belongs to',\n" //NOLINT
    "    `poolID`            int           NOT NULL COMMENT 'pool id it belongs to',\n" //NOLINT
    "    `desc`              varchar(128)  NOT NULL COMMENT 'description of server',\n" //NOLINT
    "\n"
    "    unique key (`hostName`)\n"
    ")COMMENT='server';";


const char CreateZoneTable[] =
    "create table if not exists `curve_zone` (\n"
    "    `zoneID`    int           NOT NULL PRIMARY KEY COMMENT 'zone id',\n"
    "    `zoneName`  char(128)     NOT NULL COMMENT 'zone name',\n"
    "    `poolID`    int           NOT NULL COMMENT 'physical pool id',\n"
    "    `desc`      varchar(128)           COMMENT 'description'\n"
    ")COMMENT='zone';";

const char CreatePhysicalPoolTable[] =
    "create table if not exists `curve_physicalpool` (\n"
    "    `physicalPoolID`      smallint        NOT NULL PRIMARY KEY COMMENT 'physical pool id',\n"  //NOLINT
    "    `physicalPoolName`    varchar(32)        NOT NULL COMMENT 'physical pool name',\n"         //NOLINT
    "    `desc`                varchar(128)             COMMENT 'description',\n"                   //NOLINT
    "\n"
    "    unique key (`physicalPoolName`)\n"
    ")COMMENT='physical pool';";

const char CreateLogicalPoolTable[] =
    " create table if not exists `curve_logicalpool` (\n"
    "    `logicalPoolID`      smallint     NOT NULL PRIMARY KEY COMMENT 'logical pool id',\n"       //NOLINT
    "    `logicalPoolName`    char(32)     NOT NULL COMMENT 'logical pool name',\n"                 //NOLINT
    "    `physicalPoolID`     int          NOT NULL COMMENT 'physical pool id',\n"                  //NOLINT
    "    `type`               tinyint      NOT NULL COMMENT 'pool type',\n"
    "    `initialScatterWidth` int         NOT NULL COMMENT 'initialScatterWidth',\n"               //NOLINT
    "    `createTime`         bigint       NOT NULL COMMENT 'create time',\n"
    "    `status`             tinyint      NOT NULL COMMENT 'status',\n"
    "    `redundanceAndPlacementPolicy`    json     NOT NULL COMMENT 'policy of redundance and placement',\n"   //NOLINT
    "    `userPolicy`         json         NOT NULL COMMENT 'user policy',\n"
    "    `availFlag`          boolean      NOT NULL COMMENT 'available flag'\n"
    ")COMMENT='logical pool';";

const char CreateCopySetTable[] =
    "create table if not exists `curve_copyset` (\n"
    "    `copySetID`          int            NOT NULL COMMENT 'copyset id',\n"
    "    `logicalPoolID`      smallint       NOT NULL COMMENT 'logical pool it belongs to',\n"          //NOLINT
    "    `epoch`              bigint         NOT NULL COMMENT 'copyset epoch',\n"  //NOLINT
    "    `chunkServerIDList`  varchar(32)    NOT NULL COMMENT 'list chunk server id the copyset has',\n" //NOLINT
    "\n"
    "    primary key (`logicalPoolID`,`copySetID`)\n"
    ")COMMENT='copyset';";

const char CreateSessionTable[] =
    " create table if not exists `curve_session` (\n"
    "   `entryID`       INT       unsigned     NOT NULL AUTO_INCREMENT  COMMENT '递增ID',\n"                                //NOLINT
    "   `sessionID`     VARCHAR(64)            NOT NULL   COMMENT  '唯一sessionID',\n"                                      //NOLINT
    "   `fileName`      VARCHAR(256)           NOT NULL   COMMENT  'session对应的fileName',\n"                                //NOLINT
    "   `leaseTime`     INT       unsigned     NOT NULL   COMMENT   'session对应的时间',\n"                                 //NOLINT
    "   `sessionStatus` TINYINT   unsigned     NOT NULL   COMMENT  'session状态，0: kSessionOK, 1: kSessionStaled, 2:ksessionDeleted',\n"   //NOLINT
    "   `createTime`    BIGINT                 NOT NULL   COMMENT '创建时间',\n"
    "   `updateTime`    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE  CURRENT_TIMESTAMP COMMENT '记录修改时间',\n" //NOLINT
    "   `clientIP`      VARCHAR(16)            NOT NULL   COMMENT '挂载客户端IP',\n"                                         //NOLINT
    "   `clientVersion`      VARCHAR(64)            NOT NULL   COMMENT '挂载客户端的版本号',\n"                                         //NOLINT
    "   PRIMARY KEY (`entryID`),\n"
    "   UNIQUE KEY (`sessionID`)\n"
    ")COMMENT='session';";

const char CreateSnapshotTable[] =
    "create table if not exists `snapshot` (\n"
    "    `uuid`             varchar(64)   NOT NULL PRIMARY KEY COMMENT 'snapshot uuid',\n"      //NOLINT
    "    `user`             varchar(64)   NOT NULL COMMENT 'snapshot owner',\n"                 //NOLINT
    "    `filename`         varchar(256)   NOT NULL COMMENT 'snapshot source file',\n"           // NOLINT
    "    `seqnum`           bigint        NOT NULL COMMENT 'snapshot file sequence number',\n"  //NOLINT
    "    `chunksize`        int           NOT NULL COMMENT 'snapshot file chunk size',\n"       //NOLINT
    "    `segmentsize`      int           NOT NULL COMMENT 'snapshot file segment size',\n"     //NOLINT
    "    `filelength`       bigint        NOT NULL COMMENT 'snapshot source file size',\n"      //NOLINT
    "    `time`             bigint        NOT NULL COMMENT 'snapshot create time',\n"           //NOLINT
    "    `status`           tinyint       NOT NULL COMMENT 'snapshotstate: done,deleting,processing,canceling,error',\n"    //NOLINT
    "    `snapdesc`         varchar(128)  NOT NULL COMMENT 'snapshot file user description'\n"  //NOLINT
    ")COMMENT='snapshot';";

const char CreateCloneTable[] =
    "create table if not exists `clone` (\n"
    "    `taskid`           varchar(64)   NOT NULL PRIMARY KEY COMMENT 'task ID',\n"            //NOLINT
    "    `user`             varchar(64)   NOT NULL COMMENT 'clone owner',\n"                    //NOLINT
    "    `tasktype`         tinyint       NOT NULL COMMENT 'clone task type(clone or recovery)',\n"           // NOLINT
    "    `src`              varchar(256)  NOT NULL COMMENT 'clone file source',\n"              //NOLINT
    "    `dest`             varchar(256)  NOT NULL COMMENT 'clone file destination',\n"         //NOLINT
    "    `originID`         bigint        NOT NULL COMMENT 'clone file source inode id',\n"     //NOLINT
    "    `destID`           bigint        NOT NULL COMMENT 'clone file destination inode id',\n"//NOLINT
    "    `time`             bigint        NOT NULL COMMENT 'clone/recovery create time',\n"           //NOLINT
    "    `filetype`         tinyint       NOT NULL COMMENT 'curvefs file or snapshot file',\n"    //NOLINT
    "    `isLazy`           boolean       NOT NULL COMMENT 'clone task sync or async',\n"    //NOLINT
    "    `nextstep`         tinyint       NOT NULL COMMENT 'clone/recovery processing step',\n"    //NOLINT
    "    `status`           tinyint       NOT NULL COMMENT 'clone/recovery task status'\n"    //NOLINT
    ")COMMENT='clone';";

const char CreateClientInfoTable[] =
    "create table if not exists `client_info` (\n"
    "    `clientIp`         varchar(16)   NOT NULL COMMENT 'client ip',\n"                //NOLINT
    "    `clientPort`       int           NOT NULL COMMENT 'client port',\n"               //NOLINT
    "UNIQUE KEY `key_ip_port` (`clientIp`, `clientPort`)\n"
    ")COMMENT='client_info';";

const char CreateDataBase[] = "create database if not exists %s;";
const size_t CreateDataBaseLen = strlen(CreateDataBase) - 2;

const char UseDataBase[] = "use %s";
const size_t UseDataBaseLen = strlen(UseDataBase) - 2;

const char DropDataBase[] = "drop database if exists %s";
const size_t DropDataBaseLen = strlen(DropDataBase) - 2;

const char Insert[] = "insert into %s %s values %s";
const size_t InsertLen = strlen(Insert) - 6;

const char QueryAll[] = "select * from %s";
const size_t QueryAllLen = strlen(QueryAll) - 2;

const char Query[] = "select * from %s where %s";
const size_t QueryLen = strlen(Query) - 4;

const char Delete[] = "delete from %s where %s";
const size_t DeleteLen = strlen(Delete) - 4;

const char Update[] = "update %s set %s where %s";
const size_t UpdateLen = strlen(Update) - 6;

const std::map<std::string, std::string> CurveMDSTables = {
    {ClusterInfoTable, CreateClusterInfoTable},
    {ChunkServerTable, CreateChunkServerTable},
    {ServerTable, CreateServerTable},
    {ZoneTable, CreateZoneTable},
    {PhysicalPoolTable, CreatePhysicalPoolTable},
    {LogicalPoolTable, CreateLogicalPoolTable},
    {CopySetTable, CreateCopySetTable},
    {SessionTable, CreateSessionTable},
    {ClientInfoTable, CreateClientInfoTable},
};

const std::map<std::string, std::string> CurveSnapshotTables = {
    {SnapshotTable, CreateSnapshotTable},
    {CloneTable, CreateCloneTable},
};
}  // namespace repo
}  // namespace curve

#endif  // SRC_REPO_SQLSTATEMENT_H_

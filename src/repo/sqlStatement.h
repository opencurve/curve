/*
 * Project: curve
 * Created Date: Wed Sep 19 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef curve_SQLSTATEMENT_H_
#define curve_SQLSTATEMENT_H_

#include <string>
#include <map>

namespace curve {
namespace repo {
const int SqlBufferLen = 1024;

const char ChunkServerTable[] = "curve_chunkserver";
const char ServerTable[] = "curve_server";
const char ZoneTable[] = "curve_zone";
const char LogicalPoolTable[] = "curve_logicalpool";
const char PhysicalPoolTable[] = "curve_physicalpool";
const char CopySetTable[] = "curve_copyset";

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
    "    `externalHostIP`    varchar(16)   NOT NULL COMMENT 'external host ip',\n"      //NOLINT
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
    "    `createTime`         bigint       NOT NULL COMMENT 'create time',\n"
    "    `status`             tinyint      NOT NULL COMMENT 'status',\n"
    "    `redundanceAndPlacementPolicy`    json     NOT NULL COMMENT 'policy of redundance and placement',\n"   //NOLINT
    "    `userPolicy`         json         NOT NULL COMMENT 'user policy'\n"
    ")COMMENT='logical pool';";

const char CreateCopySetTable[] =
    "create table if not exists `curve_copyset` (\n"
    "    `copySetID`          int            NOT NULL COMMENT 'copyset id',\n"
    "    `logicalPoolID`      smallint       NOT NULL COMMENT 'logical pool it belongs to',\n"          //NOLINT
    "    `chunkServerIDList`  varchar(32)   NOT NULL COMMENT 'list chunk server id the copyset has',\n" //NOLINT
    "\n"
    "    primary key (`logicalPoolID`,`copySetID`)\n"
    ")COMMENT='copyset';";

const char CreateDataBase[] = "create database if not exists %s;";

const char UseDataBase[] = "use %s";

const char DropDataBase[] = "drop database if exists %s";

const std::map<std::string, std::string> CurveTables = {
    {ChunkServerTable, CreateChunkServerTable},
    {ServerTable, CreateServerTable},
    {ZoneTable, CreateZoneTable},
    {PhysicalPoolTable, CreatePhysicalPoolTable},
    {LogicalPoolTable, CreateLogicalPoolTable},
    {CopySetTable, CreateCopySetTable},
};
}  // namespace repo
}  // namespace curve

#endif  // curve_SQLSTATEMENT_H_

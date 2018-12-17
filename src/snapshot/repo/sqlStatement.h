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
#include <cstring>

namespace curve {
namespace snapshotserver {

const char SnapshotTable[] = "snapshot";

const char CreateSnapshotTable[] =
    "create table if not exists `snapshot` (\n"
    "    `uuid`             varchar(64)   NOT NULL PRIMARY KEY COMMENT 'snapshot uuid',\n"      //NOLINT
    "    `user`             varchar(64)   NOT NULL COMMENT 'snapshot owner',\n"   //NOLINT
    "    `filename`         varchar(64)   NOT NULL COMMENT 'snapshot source file',\n" // NOLINT
    "    `seqnum`           bigint        NOT NULL COMMENT 'snapshot file sequence number',\n" //NOLINT
    "    `chunksize`        int           NOT NULL COMMENT 'snapshot file chunk size',\n"  //NOLINT
    "    `segmentsize`      int           NOT NULL COMMENT 'snapshot file segment size',\n"    //NOLINT
    "    `filelength`       bigint        NOT NULL COMMENT 'snapshot source file size',\n"         //NOLINT
    "    `time`             bigint        NOT NULL COMMENT 'snapshot create time',\n"   //NOLINT
    "    `status`           tinyint       NOT NULL COMMENT 'snapshotstate: done,deleting,processing,canceling,error',\n"    //NOLINT
    "    `snapdesc`             varchar(128)  NOT NULL COMMENT 'snapshot file user description'\n"      //NOLINT
    ")COMMENT='snapshot';";

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

const std::map<std::string, std::string> CurveTables = {
    {SnapshotTable, CreateSnapshotTable},
};
}  // namespace snapshotserver
}  // namespace curve

#endif  // curve_SQLSTATEMENT_H_

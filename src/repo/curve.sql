create database if not exists curve_mds;
use curve_mds;

create table if not exists `curve_chunkserver` (
    `chunkServerID`     int            NOT NULL PRIMARY KEY COMMENT 'chunk server id',
    `token`             varchar(16)    NOT NULL COMMENT 'token to identity chunk server',
    `diskType`          varchar(8)     NOT NULL COMMENT 'disk type',
    `internalHostIP`    varchar(16)    NOT NULL COMMENT 'internal ip',
    `port`              int            NOT NULL COMMENT 'port',
    `rwstatus`          tinyint        NOT NULL COMMENT 'chunk server status: readwrite/readonly/writeonly/pending/retired',
    `serverID`          int            NOT NULL COMMENT 'server where chunkserver in',
    `onlineState`       tinyint        NOT NULL COMMENT 'chunk server state: online/offline',
    `diskState`         tinyint        NOT NULL COMMENT 'disk state: DiskError, DiskNormal',
    `mountPoint`        varchar(32)    NOT NULL COMMENT 'disk mount point, e.g /mnt/ssd1',
    `capacity`          bigint         NOT NULL COMMENT 'total size of disk',
    `used`              bigint         NOT NULL COMMENT 'used space'
)COMMENT='chunk server';

create table if not exists `curve_server` (
    `logicalPoolID`      smallint     NOT NULL PRIMARY KEY COMMENT 'logical pool id',
    `logicalPoolName`    char(32)     NOT NULL COMMENT 'logical pool name',
    `physicalPoolID`     int          NOT NULL COMMENT 'physical pool id',
    `type`               tinyint      NOT NULL COMMENT 'pool type',
    `createTime`         bigint       NOT NULL COMMENT 'create time',
    `status`             tinyint      NOT NULL COMMENT 'status',
    `redundanceAndPlacementPolicy`    json     NOT NULL COMMENT 'policy of redundance and placement',
    `userPolicy`         json         NOT NULL COMMENT 'user policy',
    `availFlag`          BOOLEAN      NOT NULL COMMENT 'available flag'

    unique key (`hostName`)
)COMMENT='server';

create table if not exists `curve_zone` (
    `zoneID`    int           NOT NULL PRIMARY KEY COMMENT 'zone id',
    `zoneName`  char(128)     NOT NULL COMMENT 'zone name',
    `poolID`    int           NOT NULL COMMENT 'physical pool id',
    `desc`      varchar(128)           COMMENT 'description'
)COMMENT='zone';

create table if not exists `curve_physicalpool` (
    `physicalPoolID`      smallint        NOT NULL PRIMARY KEY COMMENT 'physical pool id',
    `physicalPoolName`    varchar(32)     NOT NULL COMMENT 'physical pool name',
    `desc`                varchar(128)             COMMENT 'description',

    unique key (`physicalPoolName`)
)COMMENT='physical pool';

 create table if not exists `curve_logicalpool` (
    `logicalPoolID`      smallint     NOT NULL PRIMARY KEY COMMENT 'logical pool id',
    `logicalPoolName`    char(32)     NOT NULL COMMENT 'logical pool name',
    `physicalPoolID`     int          NOT NULL COMMENT 'physical pool id',
    `type`               tinyint      NOT NULL COMMENT 'pool type',
    `initialScatterWidth` int         NOT NULL COMMENT 'initialScatterWidth',
    `createTime`         bigint       NOT NULL COMMENT 'create time',
    `status`             tinyint      NOT NULL COMMENT 'status',
    `redundanceAndPlacementPolicy`    json     NOT NULL COMMENT 'policy of redundance and placement',
    `userPolicy`         json         NOT NULL COMMENT 'user policy',
    `availFlag`          BOOLEAN      NOT NULL COMMENT 'available flag'
)COMMENT='logical pool';


create table if not exists `curve_copyset` (
    `copySetID`          int            NOT NULL COMMENT 'copyset id',
    `logicalPoolID`      smallint       NOT NULL COMMENT 'logical pool it belongs to',
    `epoch`              bigint         NOT NULL COMMENT 'copyset epoch',
    `chunkServerIDList`  varchar(32)    NOT NULL COMMENT 'list chunk server id the copyset has',

    primary key (`logicalPoolID`,`copySetID`)
)COMMENT='copyset';

create table if not exists `curve_session` (
    `entryID`       INT       unsigned     NOT NULL AUTO_INCREMENT  COMMENT '递增ID',
    `sessionID`     VARCHAR(64)            NOT NULL   COMMENT  '唯一sessionID',
    `fileName`      VARCHAR(256)           NOT NULL   COMMENT  'session对应的fileName',
    `leaseTime`     INT       unsigned     NOT NULL   COMMENT   'session对应的时间',
    `sessionStatus` TINYINT   unsigned     NOT NULL   COMMENT  'session状态，0: kSessionOK, 1: kSessionStaled, 2:ksessionDeleted',
    `createTime`    BIGINT                 NOT NULL   COMMENT '创建时间',
    `updateTime`    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE  CURRENT_TIMESTAMP COMMENT '记录修改时间',
    `clientIP`      VARCHAR(16)            NOT NULL   COMMENT '挂载客户端IP',
    PRIMARY KEY (`entryID`),
    UNIQUE KEY (`sessionID`)
)COMMENT='session';

create table if not exists `client_info` (
    `clientIp`         varchar(16)   NOT NULL COMMENT 'client ip',
    `clientPort`       int           NOT NULL COMMENT 'client port',
    UNIQUE KEY `key_ip_port` (`clientIp`, `clientPort`)
)COMMENT='client_info';
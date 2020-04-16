create database if not exists curve_snapshot;
use curve_snapshot;

create table if not exists `snapshot` (
    `uuid`             varchar(64)   NOT NULL PRIMARY KEY COMMENT 'snapshot uuid',
    `user`             varchar(64)   NOT NULL COMMENT 'snapshot owner',
    `filename`         varchar(256)  NOT NULL COMMENT 'snapshot source file',
    `seqnum`           bigint        NOT NULL COMMENT 'snapshot file sequence number',
    `chunksize`        int           NOT NULL COMMENT 'snapshot file chunk size',
    `segmentsize`      int           NOT NULL COMMENT 'snapshot file segment size',
    `filelength`       bigint        NOT NULL COMMENT 'snapshot source file size',
    `time`             bigint        NOT NULL COMMENT 'snapshot create time',
    `status`           tinyint       NOT NULL COMMENT 'snapshotstate: done,deleting,processing,canceling,error',
    `snapdesc`         varchar(128)  NOT NULL COMMENT 'snapshot file user description'
)COMMENT='snapshot';

create table if not exists `clone` (
    `taskid`           varchar(64)   NOT NULL PRIMARY KEY COMMENT 'task ID',
    `user`             varchar(64)   NOT NULL COMMENT 'clone owner',
    `tasktype`         tinyint       NOT NULL COMMENT 'clone task type(clone or recovery)',
    `src`              varchar(256)  NOT NULL COMMENT 'clone file source',
    `dest`             varchar(256)  NOT NULL COMMENT 'clone file destination',
    `originID`         bigint        NOT NULL COMMENT 'clone file source inode id',
    `destID`           bigint        NOT NULL COMMENT 'clone file destination inode id',
    `time`             bigint        NOT NULL COMMENT 'clone/recovery create time',
    `filetype`         tinyint       NOT NULL COMMENT 'curvefs file or snapshot file',
    `isLazy`           boolean       NOT NULL COMMENT 'clone task sync or async',
    `nextstep`         tinyint       NOT NULL COMMENT 'clone/recovery processing step',
    `status`           tinyint       NOT NULL COMMENT 'clone/recovery task status'
)COMMENT='clone';

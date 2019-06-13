/*
 * Project: curve
 * File Created: Monday, 13th February 2019 9:47:08 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVESNAPSHOT_PYTHON_LIBCURVESNAPSHOT_H_
#define CURVESNAPSHOT_PYTHON_LIBCURVESNAPSHOT_H_

#include <unistd.h>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct u_int32 {
    uint32_t value;
} type_uInt32_t;

typedef struct u_int64 {
    uint64_t value;
} type_uInt64_t;

typedef struct CUserInfo {
    char owner[256];
    char password[256];
} CUserInfo_t;

typedef struct FileInfo {
    type_uInt64_t      id;
    type_uInt64_t      parentid;
    int           filetype;
    type_uInt64_t      length;
    type_uInt64_t      ctime;
} FileInfo_t;

enum CFileStatus {
    Created = 0,
    Deleting,
    Cloning,
    CloneMetaInstalled,
    Cloned
};

enum CFileType {
    INODE_DIRECTORY_C = 0,
    INODE_PAGEFILE_C = 1,
    INODE_APPENDFILE_C = 2,
    INODE_APPENDECFILE_C = 3,
};

typedef struct CChunkIDInfo {
    type_uInt64_t    cid_;
    type_uInt32_t    cpid_;
    type_uInt32_t    lpid_;
} CChunkIDInfo_t;

// 保存每个chunk对应的版本信息
typedef struct CChunkInfoDetail {
    type_uInt64_t snSize;
    std::vector<int> chunkSn;
} CChunkInfoDetail_t;


// 保存logicalpool中segment对应的copysetid信息
typedef struct CLogicalPoolCopysetIDInfo {
    type_uInt32_t lpid;
    type_uInt32_t cpidVecSize;
    std::vector<int> cpidVec;
} LogicalPoolCopysetIDInfo_t;

// 保存每个segment的基本信息
typedef struct CSegmentInfo {
    type_uInt32_t segmentsize;
    type_uInt32_t chunksize;
    type_uInt64_t startoffset;
    type_uInt32_t chunkVecSize;
    std::vector<CChunkIDInfo_t> chunkvec;
    CLogicalPoolCopysetIDInfo lpcpIDInfo;
} CSegmentInfo_t;

typedef struct CFInfo {
    type_uInt64_t        id;
    type_uInt64_t        parentid;
    CFileType            filetype;
    type_uInt32_t        chunksize;
    type_uInt32_t        segmentsize;
    type_uInt64_t        length;
    type_uInt64_t        ctime;
    type_uInt64_t        seqnum;
    char                owner[256];
    char                filename[256];
    CFileStatus         filestatus;
} CFInfo_t;

int Init(const char* path);
/**
 * 创建快照
 * @param: userinfo是用户信息
 * @param: filename为要创建快照的文件名
 * @param: seq是出参，获取该文件的版本信息
 * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
 */
int CreateSnapShot(const char* filename,
                   const CUserInfo_t userinfo,
                   type_uInt64_t* seq);
/**
 * 删除快照
 * @param: userinfo是用户信息
 * @param: filename为要删除的文件名
 * @param: seq该文件的版本信息
 * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
 */
int DeleteSnapShot(const char* filename,
                   const CUserInfo_t userinfo,
                   type_uInt64_t seq);
/**
 * 获取快照对应的文件信息
 * @param: userinfo是用户信息
 * @param: filename为对应的文件名
 * @param: seq为该文件打快照时对应的版本信息
 * @param: snapinfo是出参，保存当前文件的基础信息
 * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
 */
int GetSnapShot(const char* fname, const CUserInfo_t userinfo,
                type_uInt64_t seq, CFInfo_t* snapinfo);
/**
 * 获取快照数据segment信息
 * @param: userinfo是用户信息
 * @param: filenam文件名
 * @param: seq是文件版本号信息
 * @param: offset是文件的偏移
 * @param：segInfo是出参，保存当前文件的快照segment信息
 * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
 */
int GetSnapshotSegmentInfo(const char* filename,
                        const CUserInfo_t userinfo,
                        type_uInt64_t seq,
                        type_uInt64_t offset,
                        CSegmentInfo *segInfo);

/**
 * 读取seq版本号的快照数据
 * @param: cidinfo是当前chunk对应的id信息
 * @param: seq是快照版本号
 * @param: offset是快照内的offset
 * @param: len是要读取的长度
 * @param: buf是读取缓冲区
 * @return: 成功返回LIBCURVE_ERROR::OK,否则LIBCURVE_ERROR::FAILED
 */
int ReadChunkSnapshot(CChunkIDInfo cidinfo,
                        type_uInt64_t seq,
                        type_uInt64_t offset,
                        type_uInt64_t len,
                        char *buf);
/**
 * 删除此次转储时产生的或者历史遗留的快照
 * 如果转储过程中没有产生快照，则修改chunk的correctedSn
 * @param: cidinfo是当前chunk对应的id信息
 * @param: correctedSeq是chunk需要修正的版本
 */
int DeleteChunkSnapshotOrCorrectSn(CChunkIDInfo cidinfo,
                                   type_uInt64_t correctedSeq);
/**
 * 获取chunk的版本信息，chunkInfo是出参
 * @param: cidinfo是当前chunk对应的id信息
 * @param: chunkInfo是快照的详细信息
 */
int GetChunkInfo(CChunkIDInfo cidinfo, CChunkInfoDetail *chunkInfo);
/**
 * 获取快照状态
 * @param: userinfo是用户信息
 * @param: filenam文件名
 * @param: seq是文件版本号信息
 */
int CheckSnapShotStatus(const char* filename,
                            const CUserInfo_t userinfo,
                            type_uInt64_t seq,
                            CFileStatus* filestatus);

/**
 * @brief lazy 创建clone chunk
 * @detail
 *  - location的格式定义为 A@B的形式。
 *  - 如果源数据在s3上，则location格式为uri@s3，uri为实际chunk对象的地址；
 *  - 如果源数据在curvefs上，则location格式为/filename/chunkindex@cs
 *
 * @param:location 数据源的url
 * @param:chunkidinfo 目标chunk
 * @param:sn chunk的序列号
 * @param:chunkSize chunk的大小
 * @param:correntSn CreateCloneChunk时候用于修改chunk的correctedSn
 *
 * @return 错误码
 */
int CreateCloneChunk(const char* location,
                            const CChunkIDInfo chunkidinfo,
                            type_uInt64_t sn,
                            type_uInt64_t correntSn,
                            type_uInt64_t chunkSize);

/**
 * @brief 实际恢复chunk数据
 *
 * @param:chunkidinfo chunkidinfo
 * @param:offset 偏移
 * @param:len 长度
 *
 * @return 错误码
 */
int RecoverChunk(const CChunkIDInfo chunkidinfo,
                            type_uInt64_t offset,
                            type_uInt64_t len);

/**
 * 析构，回收资源
 */
void UnInit();

#ifdef __cplusplus
}
#endif
#endif  // CURVESNAPSHOT_PYTHON_LIBCURVESNAPSHOT_H_

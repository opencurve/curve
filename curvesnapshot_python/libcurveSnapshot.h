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
 * File Created: Monday, 13th February 2019 9:47:08 am
 * Author: tongguangxun
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

enum CFileType {
    INODE_DIRECTORY_C = 0,
    INODE_PAGEFILE_C = 1,
    INODE_APPENDFILE_C = 2,
    INODE_APPENDECFILE_C = 3,
    INODE_SNAPSHOT_PAGEFILE_C = 4
};

typedef struct FileInfo {
    type_uInt64_t id;
    type_uInt64_t parentid;
    int filetype;
    type_uInt64_t length;
    type_uInt64_t ctime;
} FileInfo_t;

enum CFileStatus { Created = 0, Deleting, Cloning, CloneMetaInstalled, Cloned };

typedef struct CChunkIDInfo {
    type_uInt64_t cid_;
    type_uInt32_t cpid_;
    type_uInt32_t lpid_;
} CChunkIDInfo_t;

// Save the version information corresponding to each chunk
typedef struct CChunkInfoDetail {
    type_uInt64_t snSize;
    std::vector<int> chunkSn;
} CChunkInfoDetail_t;

// Save the copysetid information corresponding to the segment in the
// logicalpool
typedef struct CLogicalPoolCopysetIDInfo {
    type_uInt32_t lpid;
    type_uInt32_t cpidVecSize;
    std::vector<int> cpidVec;
} LogicalPoolCopysetIDInfo_t;

// Save basic information for each segment
typedef struct CSegmentInfo {
    type_uInt32_t segmentsize;
    type_uInt32_t chunksize;
    type_uInt64_t startoffset;
    type_uInt32_t chunkVecSize;
    std::vector<CChunkIDInfo_t> chunkvec;
    CLogicalPoolCopysetIDInfo lpcpIDInfo;
} CSegmentInfo_t;

typedef struct CFInfo {
    type_uInt64_t id;
    type_uInt64_t parentid;
    CFileType filetype;
    type_uInt32_t chunksize;
    type_uInt32_t segmentsize;
    type_uInt64_t length;
    type_uInt64_t ctime;
    type_uInt64_t seqnum;
    char owner[256];
    char filename[256];
    CFileStatus filestatus;
} CFInfo_t;

int Init(const char* path);
/**
 * Create a snapshot
 * @param: userinfo is the user information
 * @param: filename is the file name to create the snapshot
 * @param: seq is the output parameter to obtain the version information of the
 * file
 * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise
 * LIBCURVE_ERROR::FAILED
 */
int CreateSnapShot(const char* filename, const CUserInfo_t userinfo,
                   type_uInt64_t* seq);
/**
 * Delete snapshot
 * @param: userinfo is the user information
 * @param: filename is the file name to be deleted
 * @param: seq The version information of this file
 * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise
 * LIBCURVE_ERROR::FAILED
 */
int DeleteSnapShot(const char* filename, const CUserInfo_t userinfo,
                   type_uInt64_t seq);
/**
 * Obtain file information corresponding to the snapshot
 * @param: userinfo is the user information
 * @param: filename is the corresponding file name
 * @param: seq corresponds to the version information when taking a snapshot of
 * the file
 * @param: snapinfo is a parameter that saves the basic information of the
 * current file
 * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise
 * LIBCURVE_ERROR::FAILED
 */
int GetSnapShot(const char* fname, const CUserInfo_t userinfo,
                type_uInt64_t seq, CFInfo_t* snapinfo);
/**
 * Obtain snapshot data segment information
 * @param: userinfo is the user information
 * @param: filenam file name
 * @param: seq is the file version number information
 * @param: offset is the offset of the file
 * @param: segInfo is a parameter that saves the snapshot segment information of
 * the current file
 * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise
 * LIBCURVE_ERROR::FAILED
 */
int GetSnapshotSegmentInfo(const char* filename, const CUserInfo_t userinfo,
                           type_uInt64_t seq, type_uInt64_t offset,
                           CSegmentInfo* segInfo);

/**
 * Read snapshot data of seq version number
 * @param: cidinfo is the ID information corresponding to the current chunk
 * @param: seq is the snapshot version number
 * @param: offset is the offset within the snapshot
 * @param: len is the length to be read
 * @param: buf is a read buffer
 * @return: Successfully returned LIBCURVE_ERROR::OK, otherwise
 * LIBCURVE_ERROR::FAILED
 */
int ReadChunkSnapshot(CChunkIDInfo cidinfo, type_uInt64_t seq,
                      type_uInt64_t offset, type_uInt64_t len, char* buf);
/**
 * Delete snapshots generated during this dump or left over from history
 * If no snapshot is generated during the dump process, modify the correctedSn
 * of the chunk
 * @param: cidinfo is the ID information corresponding to the current chunk
 * @param: correctedSeq is the version of chunk that needs to be corrected
 */
int DeleteChunkSnapshotOrCorrectSn(CChunkIDInfo cidinfo,
                                   type_uInt64_t correctedSeq);
/**
 * Obtain the version information of the chunk, where chunkInfo is the output
 * parameter
 * @param: cidinfo is the ID information corresponding to the current chunk
 * @param: chunkInfo is the detailed information of the snapshot
 */
int GetChunkInfo(CChunkIDInfo cidinfo, CChunkInfoDetail* chunkInfo);
/**
 * Get snapshot status
 * @param: userinfo is the user information
 * @param: filenam file name
 * @param: seq is the file version number information
 */
int CheckSnapShotStatus(const char* filename, const CUserInfo_t userinfo,
                        type_uInt64_t seq, type_uInt32_t* filestatus);
/**
 * Obtain snapshot allocation information
 * @param: filename is the current file name
 * @param: offset is the current file offset
 * @param: segmentsize is the segment size
 * @param: chunksize
 * @param: userinfo is the user information
 * @param[out]: segInfo is the output parameter
 */
int GetOrAllocateSegmentInfo(const char* filename, type_uInt64_t offset,
                             type_uInt64_t segmentsize, type_uInt64_t chunksize,
                             const CUserInfo_t userinfo, CSegmentInfo* segInfo);
/**
 * @brief lazy Create clone chunk
 * @detail
 *  - The format of 'location' is defined as A@B.
 *  - If the source data is on S3, the 'location' format is uri@s3, where 'uri'
 * is the actual address of the chunk object.
 *  - If the source data is on CurveFS, the 'location' format is
 * /filename/chunkindex@cs.
 *
 * @param: location    The URL of the data source
 * @param: chunkidinfo The target chunk
 * @param: sn          The sequence number of the chunk
 * @param: chunkSize   The size of the chunk
 * @param: correntSn   Used for modifying the 'correctedSn' when creating the
 * clone chunk
 *
 * @return error code
 */
int CreateCloneChunk(const char* location, const CChunkIDInfo chunkidinfo,
                     type_uInt64_t sn, type_uInt64_t correntSn,
                     type_uInt64_t chunkSize);

/**
 * @brief Actual recovery chunk data
 *
 * @param: chunkidinfo chunkidinfo
 * @param: offset offset
 * @param: len length
 *
 * @return error code
 */
int RecoverChunk(const CChunkIDInfo chunkidinfo, type_uInt64_t offset,
                 type_uInt64_t len);

/**
 * Deconstruct and recycle resources
 */
void UnInit();

#ifdef __cplusplus
}
#endif
#endif  // CURVESNAPSHOT_PYTHON_LIBCURVESNAPSHOT_H_

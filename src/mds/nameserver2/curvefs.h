/*
 * Project: curve
 * Created Date: Friday September 7th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_CURVEFS_H_
#define SRC_MDS_NAMESERVER2_CURVEFS_H_

#include <vector>
#include <string>
#include "proto/nameserver2.pb.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/nameserver2/inode_id_generator.h"
#include "src/mds/nameserver2/define.h"
#include "src/mds/nameserver2/chunk_allocator.h"
#include "src/mds/nameserver2/clean_manager.h"
#include "src/mds/nameserver2/async_delete_snapshot_entity.h"

namespace curve {
namespace mds {


const uint64_t ROOTINODEID = 0;
const char ROOTFILENAME[] = "/";
const uint64_t INTERALGARBAGEDIR = 1;

using ::curve::mds::DeleteSnapShotResponse;

class CurveFS {
 public:
    // singleton, supported in c++11
    static CurveFS &GetInstance() {
        static CurveFS curvefs;
        return curvefs;
    }
    void Init(NameServerStorage*,
            InodeIDGenerator*,
            ChunkSegmentAllocator*,
            std::shared_ptr<CleanManagerInterface>);

    // namespace ops
    StatusCode CreateFile(const std::string & fileName,
                          FileType filetype,
                          uint64_t length);

    StatusCode GetFileInfo(const std::string & filename,
                           FileInfo * inode) const;

    StatusCode DeleteFile(const std::string & filename);

    StatusCode ReadDir(const std::string & dirname,
                       std::vector<FileInfo> * files) const;

    StatusCode RenameFile(const std::string & oldFileName,
                          const std::string & newFileName);

    // extent size minimum unit 1GB ( segement as a unit)
    StatusCode ExtendFile(const std::string &filename,
                          uint64_t newSize);


    // segment(chunk) ops
    StatusCode GetOrAllocateSegment(
        const std::string & filename,
        offset_t offset,
        bool allocateIfNoExist, PageFileSegment *segment);

    StatusCode DeleteSegment(const std::string &filename, offset_t offset);

    FileInfo GetRootFileInfo(void) const {
        return rootFileInfo_;
    }

    StatusCode CreateSnapShotFile(const std::string &fileName,
                            FileInfo *snapshotFileInfo);
    StatusCode ListSnapShotFile(const std::string & fileName,
                            std::vector<FileInfo> *snapshotFileInfos) const;
    // async interface
    StatusCode DeleteFileSnapShotFile(const std::string &fileName,
                            FileSeqType seq,
                            std::shared_ptr<AsyncDeleteSnapShotEntity> entity);
    StatusCode CheckSnapShotFileStatus(const std::string &fileName,
                            FileSeqType seq, FileStatus * status,
                            uint32_t * progress) const;

    StatusCode GetSnapShotFileInfo(const std::string &fileName,
                            FileSeqType seq, FileInfo *snapshotFileInfo) const;

    StatusCode GetSnapShotFileSegment(
            const std::string & filename,
            FileSeqType seq,
            offset_t offset,
            PageFileSegment *segment);

 private:
    CurveFS() = default;

    void InitRootFile(void);

    StatusCode WalkPath(const std::string &fileName,
                        FileInfo *fileInfo, std::string  *lastEntry) const;

    StatusCode LookUpFile(const FileInfo & parentFileInfo,
                          const std::string & fileName,
                          FileInfo *fileInfo) const;

    StatusCode PutFile(const FileInfo & fileInfo);

    /**
     * @brief 执行一次fileinfo的snapshot快照事务
     * @param originalFileInfo: 原文件对于fileInfo
     * @param SnapShotFile: 生成的snapshot文件对于fileInfo
     * @return StatusCode: 成功或者失败
     */
    StatusCode SnapShotFile(const FileInfo * originalFileInfo,
        const FileInfo * SnapShotFile) const;

 private:
    FileInfo rootFileInfo_;
    NameServerStorage*          storage_;
    InodeIDGenerator*           InodeIDGenerator_;
    ChunkSegmentAllocator*      chunkSegAllocator_;

    std::shared_ptr<CleanManagerInterface> snapshotCleanManager_;
};
extern CurveFS &kCurveFS;
}   // namespace mds
}   // namespace curve
#endif   // SRC_MDS_NAMESERVER2_CURVEFS_H_


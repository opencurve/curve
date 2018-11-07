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

namespace curve {
namespace mds {


const uint64_t ROOTINODEID = 0;
const char ROOTFILENAME[] = "/";
const uint64_t INTERALGARBAGEDIR = 1;

class CurveFS {
 public:
    // singleton, supported in c++11
    static CurveFS &GetInstance() {
        static CurveFS curvefs;
        return curvefs;
    }
    void Init(NameServerStorage*, InodeIDGenerator*, ChunkSegmentAllocator*);

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
    // TODO(hzsunjianliang): snapshot ops
 private:
    CurveFS() = default;

    void InitRootFile(void);

    StatusCode WalkPath(const std::string &fileName,
                        FileInfo *fileInfo, std::string  *lastEntry) const;

    StatusCode LookUpFile(const FileInfo & parentFileInfo,
                          const std::string & fileName,
                          FileInfo *fileInfo) const;

    StatusCode PutFile(const FileInfo & fileInfo);

 private:
    FileInfo rootFileInfo_;
    NameServerStorage*          storage_;
    InodeIDGenerator*           InodeIDGenerator_;
    ChunkSegmentAllocator*      chunkSegAllocator_;
};

extern CurveFS &kCurveFS;

}   // namespace mds
}   // namespace curve
#endif   // SRC_MDS_NAMESERVER2_CURVEFS_H_


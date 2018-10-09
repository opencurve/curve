/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include <string>
#include <vector>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/configuration.h"
#include "proto/integrity.pb.h"


#ifndef SRC_CHUNKSERVER_INTEGRITY_H_
#define SRC_CHUNKSERVER_INTEGRITY_H_

namespace curve {
namespace chunkserver {

using JobID     = std::string;

struct IntegrityOptions {
};

class IntegrityCheckJob {
 public:
    IntegrityCheckJob() {}
    ~IntegrityCheckJob() {}

    int VerifyChunks();

 private:
    JobID                   job;
    INTEGRITY_JOB_STATE     state;
    LogicPoolID                  lastPool;
    CopysetID               lastCopyset;
    ChunkID                 lastChunk;
    std::vector<ChunkID>    badChunks;
};

class Integrity {
 public:
    Integrity() {}
    ~Integrity() {}

    int Init(const IntegrityOptions &options);
    int Run();
    int Fini();

    // Task management
    int StartJob();
    int StopJob();
    int GetJob();

 private:
    int InitConfig();

    IntegrityOptions    options_;
    IntegrityCheckJob   job_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_INTEGRITY_H_

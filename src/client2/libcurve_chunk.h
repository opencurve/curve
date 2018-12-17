/*
 * Project: curve
 * File Created: Monday, 13th February 2019 9:47:08 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_LIBCURVE_CHUNK_H
#define CURVE_LIBCURVE_CHUNK_H

#include <unistd.h>
#include <string>
#include <atomic>
#include <vector>
#include <unordered_map>

#include "src/client2/config_info.h"
#include "src/client2/client_common.h"

namespace curve {
namespace client {
class SnapInstance;
class SnapshotClient {
 public:
  SnapshotClient();
  ~SnapshotClient() = default;
  /**
   * 1. when libcurve provide by library, sush as qemu
   *    we need invoker pass config path.
   * 2. when libcurve provide by source code, such as snapshotter
   *    we need invoker pass config option
   */
  int Init(const char* configpath);
  int Init(ClientConfigOption_t opt);
  int CreateSnapShot(std::string filename, uint64_t* seq);
  int DeleteSnapShot(std::string filename, uint64_t seq);
  int GetSnapShot(std::string filename, uint64_t seq, FInfo* snapinfo);
  int ListSnapShot(std::string filename,
                        const std::vector<uint64_t>* seq,
                        std::vector<FInfo*>* snapif);
  int GetSnapshotSegmentInfo(std::string filename,
                        uint64_t seq,
                        uint64_t offset,
                        SegmentInfo *segInfo);
  int ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        void *buf);
  int DeleteChunkSnapshot(ChunkIDInfo cidinfo, uint64_t seq);
  int GetChunkInfo(ChunkIDInfo cidinfo, ChunkInfoDetail *chunkInfo);
  void UnInit();

 private:
  SnapInstance* snapinstance_;
};
}   // namespace client
}   // namespace curve
#endif  // !CURVE_LIBCURVE_H

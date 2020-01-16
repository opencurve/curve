#ifndef SRC_PART1_NEBD_METACACHE_H_
#define SRC_PART1_NEBD_METACACHE_H_

#include <unordered_map>
#include <vector>
#include <string>
#include "src/common/rw_lock.h"

namespace nebd {
namespace client {

struct NebdClientFileInfo {
    int fd;
    std::string fileName;
};

class NebdClientMetaCache {
 public:
    NebdClientMetaCache();
    ~NebdClientMetaCache();
    void AddFileInfo(NebdClientFileInfo fileInfo);
    void RemoveFileInfo(NebdClientFileInfo fileInfo);
    std::vector<NebdClientFileInfo> GetAllFileInfo();

 private:
    std::unordered_map<int, NebdClientFileInfo> fileinfos_;
    common::RWLock rwLock_;
};

}  // namespace client
}  // namespace nebd

#endif  // SRC_PART1_NEBD_METACACHE_H_

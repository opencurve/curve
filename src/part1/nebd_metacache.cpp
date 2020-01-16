#include "src/part1/nebd_metacache.h"

namespace nebd {
namespace client {

NebdClientMetaCache::NebdClientMetaCache() {}
NebdClientMetaCache::~NebdClientMetaCache() {}

void NebdClientMetaCache::AddFileInfo(NebdClientFileInfo fileInfo) {
    //  TODO 
}

void NebdClientMetaCache::RemoveFileInfo(NebdClientFileInfo fileInfo) {
    //  TODO 
}

std::vector<NebdClientFileInfo> NebdClientMetaCache::GetAllFileInfo() {
    //  TODO 
    std::vector<NebdClientFileInfo> result;
    return result;
}

}  // namespace client
}  // namespace nebd

#include "src/part2/metafile_manager.h"

namespace nebd {
namespace server {
    
NebdMetaFileManager::NebdMetaFileManager(const std::string& metaFilePath)
    : metaFilePath_(metaFilePath) {
    // TODO
}

NebdMetaFileManager::~NebdMetaFileManager() {}

int NebdMetaFileManager::RemoveFileInfo(const std::string& fileName) {
    // TODO 
    return 0;
}

int NebdMetaFileManager::UpdateFileInfo(const NebdFileInfo& fileInfo) {
    // TODO
    return 0;
}

int NebdMetaFileManager::ListFileInfo(std::vector<NebdFileInfo>* fileInfos) {
    // TODO
    return 0;
}

}  // namespace server
}  // namespace nebd
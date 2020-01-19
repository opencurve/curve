#include "src/part2/metafile_manager.h"

namespace nebd {
namespace server {

NebdMetaFileManager::NebdMetaFileManager(const std::string& metaFilePath)
    : metaFilePath_(metaFilePath) {
    // TODO
}

NebdMetaFileManager::~NebdMetaFileManager() {}

int NebdMetaFileManager::RemoveFileRecord(const std::string& fileName) {
    // TODO
    return 0;
}

int NebdMetaFileManager::UpdateFileRecord(const NebdFileRecordPtr& fileRecord) {
    // TODO
    return 0;
}

int NebdMetaFileManager::ListFileRecord(std::vector<NebdFileRecordPtr>* fileRecords) {
    // TODO
    return 0;
}

}  // namespace server
}  // namespace nebd
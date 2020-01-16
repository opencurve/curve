#ifndef SRC_PART2_METAFILE_MANAGER_H_
#define SRC_PART2_METAFILE_MANAGER_H_

#include <string>
#include <vector>

#include "src/part2/define.h"

namespace nebd {
namespace server {

class NebdMetaFileManager {
 public:
    explicit NebdMetaFileManager(const std::string& metaFilePath);
    virtual ~NebdMetaFileManager();

    virtual int RemoveFileInfo(const std::string& fileName);
    virtual int UpdateFileInfo(const NebdFileInfo& fileInfo);
    virtual int ListFileInfo(std::vector<NebdFileInfo>* fileInfos);

 private:
    std::string metaFilePath_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_METAFILE_MANAGER_H_

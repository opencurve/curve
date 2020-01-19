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

    virtual int RemoveFileRecord(const std::string& fileName);
    virtual int UpdateFileRecord(const NebdFileRecordPtr& fileRecord);
    virtual int ListFileRecord(std::vector<NebdFileRecordPtr>* fileRecords);

 private:
    std::string metaFilePath_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_METAFILE_MANAGER_H_

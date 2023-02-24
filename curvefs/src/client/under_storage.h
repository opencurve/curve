/*
#ifndef CURVEFS_SRC_CLIENT_UNDER_STORAGE_H_
#define CURVEFS_SRC_CLIENT_UNDER_STORAGE_H_


#include "curvefs/src/volume/common.h"

using ::curvefs::volume::ReadPart;

namespace curvefs {
namespace client {

class UnderStorage {
 public:
    UnderStorage() {}
    virtual ~UnderStorage() = default;
  
    virtual ssize_t Write(uint64_t ino, off_t offset, size_t len, const char* data) = 0;
   
    virtual ssize_t Read(uint64_t ino, off_t offset, size_t len, const char* data, std::vector<ReadPart> *miss) = 0; 
  
    virtual CURVEFS_ERROR Truncate(uint64_t ino, size_t size) = 0;               
};

}
}
#endif  // CURVEFS_SRC_CLIENT_UNDER_STORAGE_H_
 */
/*
 * Project: curve
 * File Created: Thursday, 27th September 2018 4:41:01 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include "src/sfs/sfsMock.h"

namespace curve {
namespace sfs {

curve::sfs::LocalFileSystem * curve::sfs::LocalFsFactory::localFs_ = nullptr;

}  // namespace sfs
}  // namespace curve

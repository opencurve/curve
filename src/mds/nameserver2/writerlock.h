/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 2022-07-25
 * Author: YangFan (fansehep)
 */

#ifndef SRC_MDS_NAMESERVER2_WRITERLOCK_H_
#define SRC_MDS_NAMESERVER2_WRITERLOCK_H_

#include <string>
#include <memory>

#include "src/common/encode.h"
#include "src/common/concurrent/name_lock.h"
#include "src/mds/nameserver2/namespace_storage.h"

namespace curve {
namespace mds {

using ::curve::common::NameLock;
using ::curve::common::Uncopyable;
using ::curve::kvstorage::EtcdClientImp;

enum class WriterStatus {
    kSuccess,
    kError,
    kNoExist,
    kExpired,
};

// current file writer able to timeout (us)
// it will read from /mds/mds.conf.
struct WriterLockTimeoutOption {
    uint32_t timeoutus;
    explicit WriterLockTimeoutOption(uint32_t timeus)
        : timeoutus(timeus) {}
    WriterLockTimeoutOption() : timeoutus(5000000) {}
};

class WriterLock {
 public:
    explicit WriterLock(std::shared_ptr<NameServerStorage> storageimp,
        WriterLockTimeoutOption timeopt)
    : timeoutopt_(timeopt),
      storage_(storageimp) {}

    /**
     * client try to lock and get write permission
     * if current file has a writer and no timeout,
     * the client will get openfile error.
     * LIBCURVE_ERROR::PERMISSION_DENY in client
     * @param:
     * @return: return true if current client
     *          open2 with write success
     */
    bool Lock(const uint64_t fileid,
              const std::string& clientuuid);

    /**
     * only the current file writer can unlock the file
     * if old writer lost write permission, other client
     * can get the current file write permission.
     * @param:
     * @return: unlock success return true
     *          otherwise return false
     */
    bool Unlock(const uint64_t fileid,
                const std::string& clientuuid);

    /**
     * only the current writer can update.
     * writer mark the sesson with mds.
     * @param:
     * @return: update success return true, otherwise
     *          return false
     */
    bool UpdateLockTime(const uint64_t fileid,
                        const std::string& clientuuid);

 private:
    WriterStatus GetWriterStatus(const uint64_t fileid,
                                 std::string* clientuuid);

    bool SetWriter(const uint64_t fileid,
                   const std::string& clientuuid);

    bool ClearWriter(const uint64_t fileid);

    NameLock namelock_;
    const WriterLockTimeoutOption timeoutopt_;
    std::shared_ptr<NameServerStorage> storage_;
};

}   //  namespace mds
}   //  namespace curve

#endif  //  SRC_MDS_NAMESERVER2_WRITERLOCK_H_

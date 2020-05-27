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
 * Created Date: 2020/11/26
 * Author: hzchenwei7
 */

#ifndef SRC_MDS_SNAPSHOTCLONECLIENT_SNAPSHOTCLONE_CLIENT_H_
#define SRC_MDS_SNAPSHOTCLONECLIENT_SNAPSHOTCLONE_CLIENT_H_

#include <string>
#include <vector>
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "proto/nameserver2.pb.h"  // for retcode StatusCode

using curve::mds::StatusCode;
using curve::snapshotcloneserver::CloneRefStatus;

namespace curve {
namespace mds {
namespace snapshotcloneclient {

struct SnapshotCloneClientOption {
    std::string snapshotCloneAddr;
};

struct DestFileInfo {
    std::string filename;
    uint64_t inodeid;
};

class SnapshotCloneClient {
 public:
    SnapshotCloneClient()
        : addr_(""), inited_(false) {}

    virtual ~SnapshotCloneClient() {}

    virtual void Init(const SnapshotCloneClientOption &option);

    /**
     * @brief get the clone ref status of a file. As a clone src file,
     *        find if has clone ref.
     *
     * @param filename the name of clone src file
     * @param user the user of clone src file
     * @param status return 0 means no ref, return 1 means has ref,
     *               return 2 means not sure, need check fileCheckList
     * @param fileCheckList the need check dest file info list
     *
     * @return error code
     */
    virtual StatusCode GetCloneRefStatus(std::string filename, std::string user,
                                  CloneRefStatus *status,
                                  std::vector<DestFileInfo> *fileCheckList);

    // test only
    virtual bool GetInitStatus();

 private:
    std::string addr_;
    bool inited_;
};

}  // namespace snapshotcloneclient
}  // namespace mds
}  // namespace curve


#endif  // SRC_MDS_SNAPSHOTCLONECLIENT_SNAPSHOTCLONE_CLIENT_H_

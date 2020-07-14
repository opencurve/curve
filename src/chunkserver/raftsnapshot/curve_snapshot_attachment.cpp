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
 * Created Date: Friday October 11th 2019
 * Author: yangyaokai
 */

#include "src/chunkserver/raftsnapshot/curve_snapshot_attachment.h"
#include "src/common/fs_util.h"

namespace curve {
namespace chunkserver {

CurveSnapshotAttachment::CurveSnapshotAttachment(
    std::shared_ptr<LocalFileSystem> fs)
    : fileHelper_(fs) {}

void CurveSnapshotAttachment::list_attach_files(
    std::vector<std::string> *files, const std::string& raftSnapshotPath) {
    std::string raftBaseDir =
                    getCurveRaftBaseDir(raftSnapshotPath, RAFT_SNAP_DIR);
    std::string dataDir;
    if (raftBaseDir[raftBaseDir.length()-1] != '/') {
        dataDir = raftBaseDir + "/" + RAFT_DATA_DIR;
    } else {
        dataDir = raftBaseDir + RAFT_DATA_DIR;
    }

    std::vector<std::string> snapFiles;
    int rc = fileHelper_.ListFiles(dataDir, nullptr, &snapFiles);
    // list出错一般认为就是磁盘出现问题了，这种情况直接让进程挂掉
    // Attention: 这里还需要更仔细考虑
    CHECK(rc == 0) << "List dir failed.";

    files->clear();
    // 文件路径格式与snapshot_meta中的格式要相同
    for (const auto& snapFile : snapFiles) {
        std::string snapApath;
        // 添加绝对路径
        snapApath.append(dataDir);
        snapApath.append("/").append(snapFile);
        std::string filePath = curve::common::CalcRelativePath(
                                    raftSnapshotPath, snapApath);
        files->emplace_back(filePath);
    }
}


}  // namespace chunkserver
}  // namespace curve

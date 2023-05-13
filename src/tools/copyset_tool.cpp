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
 * Created Date: 2021-02-03
 * Author: charisu
 */

#include "src/tools/copyset_tool.h"

DEFINE_bool(dryrun, true, "when dry run set true, no changes will be made");
DEFINE_bool(availFlag, false, "copysets available flag");

DECLARE_string(mdsAddr);

namespace curve {
namespace tool {

int CopysetTool::RunCommand(const std::string& command) {
    if (Init() != 0) {
        std::cout << "Init CopysetTool failed" << std::endl;
        return -1;
    }
    if (command == kSetCopysetAvailFlag) {
        google::CommandLineFlagInfo info;
        if (!GetCommandLineFlagInfo("availFlag", &info)) {
            std::cout << "availFlag must be specified!" << std::endl;
            return -1;
        }
        if (FLAGS_availFlag) {
            return SetCopysetsAvailable();
        } else {
            return SetCopysetsUnAvailable();
        }
    } else {
        PrintHelp(command);
        return -1;
    }
}

void CopysetTool::PrintHelp(const std::string& command) {
    std::cout << "Example: " << std::endl;
    if (command == kSetCopysetAvailFlag) {
        std::cout << "curve_ops_tool " << kSetCopysetAvailFlag
                  << " -dryrun=true/false -availFlag=true/false" << std::endl;
    } else {
        std::cout << "command not supported!" << std::endl;
    }
}

bool CopysetTool::SupportCommand(const std::string& command) {
    return command == kSetCopysetAvailFlag;
}

int CopysetTool::Init() {
    if (!inited_) {
        int res = mdsClient_->Init(FLAGS_mdsAddr);
        if (res != 0) {
            std::cout << "Init copysetCheckCore fail!" << std::endl;
            return -1;
        }
        res = copysetCheck_->Init(FLAGS_mdsAddr);
        if (res != 0) {
           std::cout << "Init copysetCheckCore fail!" << std::endl;
            return -1;
        }
        inited_ = true;
    }
    return 0;
}

void PrintCopysets(const std::vector<CopysetInfo>& copysets) {
    for (size_t i = 0; i < copysets.size(); ++i) {
        if (i != 0) {
            std::cout << ",";
        }
        std::cout << "(" << copysets[i].logicalpoolid() << ","
                  << copysets[i].copysetid() << ")";
    }
    std::cout << std::endl;
}

int CopysetTool::SetCopysetsUnAvailable() {
    int res = copysetCheck_->CheckCopysetsOnOfflineChunkServer();
    if (res != 0) {
        std::cout << "CheckCopysetsOnOfflineChunkServer fail" << std::endl;
        return -1;
    }
    std::vector<CopysetInfo> copysetsToSet;
    copysetCheck_->GetCopysetInfos(kMajorityPeerNotOnline, &copysetsToSet);
    if (copysetsToSet.empty()) {
        std::cout << "No majority-peers-offline copysets" << std::endl;
        return 0;
    }
    if (FLAGS_dryrun) {
        PrintCopysets(copysetsToSet);
        return 0;
    }
    res = mdsClient_->SetCopysetsAvailFlag(copysetsToSet, false);
    if (res != 0) {
        std::cout << "SetCopysetsAvailFlag fail" << std::endl;
        return -1;
    }
    return 0;
}

int CopysetTool::SetCopysetsAvailable() {
    std::vector<CopysetInfo> copysets;
    int res = mdsClient_->ListUnAvailCopySets(&copysets);
    if (res != 0) {
        std::cout << "ListUnAvailCopySets fail!" << std::endl;
        return -1;
    }
    std::vector<CopysetInfo> copysetsToSet;
    for (const auto& copyset : copysets) {
        auto res = copysetCheck_->CheckOneCopyset(copyset.logicalpoolid(),
                                   copyset.copysetid());
        if (res != CheckResult::kMajorityPeerNotOnline) {
            copysetsToSet.emplace_back(copyset);
        }
    }
    if (copysetsToSet.empty()) {
        std::cout << "No unavailable copysets" << std::endl;
        return 0;
    }
    if (FLAGS_dryrun) {
        PrintCopysets(copysetsToSet);
        return 0;
    }
    res = mdsClient_->SetCopysetsAvailFlag(copysetsToSet, true);
    if (res != 0) {
        std::cout << "SetCopysetsAvailFlag true fail!" << std::endl;
        return -1;
    }
    return 0;
}

}  // namespace tool
}  // namespace curve

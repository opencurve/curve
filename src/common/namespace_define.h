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
 * Created Date: 2020-03-13
 * Author: lixiaocui
 */

#ifndef SRC_COMMON_NAMESPACE_DEFINE_H_
#define SRC_COMMON_NAMESPACE_DEFINE_H_

namespace curve {
namespace common {

const char FILEINFOKEYPREFIX[] = "01";
const char FILEINFOKEYEND[] = "02";
const char SEGMENTINFOKEYPREFIX[] = "02";
const char SEGMENTINFOKEYEND[] = "03";
const char SNAPSHOTFILEINFOKEYPREFIX[] = "03";
const char SNAPSHOTFILEINFOKEYEND[] = "04";
const char INODESTOREKEY[] = "04";
const char INODESTOREKEYEND[] = "05";
const char CHUNKSTOREKEY[] = "05";
const char CHUNKSTOREKEYEND[] = "06";
const char LEADERCAMPAIGNNPFX[] = "07leader";
const char SEGMENTALLOCSIZEKEY[] = "08";
const char SEGMENTALLOCSIZEKEYEND[] = "09";

const char LOGICALPOOLKEYPREFIX[] = "1001";
const char LOGICALPOOLKEYEND[] = "1002";
const char PHYSICALPOOLKEYPREFIX[] = "1002";
const char PHYSICALPOOLKEYEND[] = "1003";
const char ZONEKEYPREFIX[] = "1003";
const char ZONEKEYEND[] = "1004";
const char SERVERKEYPREFIX[] = "1004";
const char SERVERKEYEND[] = "1005";
const char CHUNKSERVERKEYPREFIX[] = "1005";
const char CHUNKSERVERKEYEND[] = "1006";
const char CLUSTERINFOKEY[] = "1007";
const char COPYSETKEYPREFIX[] = "1008";
const char COPYSETKEYEND[] = "1009";

const char SNAPINFOKEYPREFIX[] = "11";
const char SNAPINFOKEYEND[] = "12";
const char CLONEINFOKEYPREFIX[] = "12";
const char CLONEINFOKEYEND[] = "13";

const char DISCARDSEGMENTKEYPREFIX[] = "13";
const char DISCARDSEGMENTKEYEND[] = "14";

// TODO(hzsunjianliang): if use single prefix for snapshot file?
const int COMMON_PREFIX_LENGTH = 2;
const int LEADER_PREFIX_LENGTH = 8;
const int SEGMENTKEYLEN = 18;
const int DISCARDSEGMENTKEYLEN = 26;
// save info for file with permission
const int PERM_PREFIX_LENGTH = 2;
const char PERMINFOPREFIX[] = "15";
}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_NAMESPACE_DEFINE_H_



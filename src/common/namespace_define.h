/*
 * Project: curve
 * Created Date: 2020-03-13
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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

// TODO(hzsunjianliang): if use single prefix for snapshot file?
const int COMMON_PREFIX_LENGTH = 2;
const int LEADER_PREFIX_LENGTH = 8;
const int SEGMENTKEYLEN = 18;

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_NAMESPACE_DEFINE_H_



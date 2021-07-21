/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thu Jul 22 10:45:43 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_CODEC_CODEC_H_
#define CURVEFS_SRC_MDS_CODEC_CODEC_H_

#include <google/protobuf/message.h>

#include <string>

namespace curvefs {
namespace mds {
namespace codec {

// Return storage key for fsname
std::string EncodeFsName(const std::string& fsName);

std::string FsNameStoreKey();

std::string FsNameStoreEndKey();

inline bool EncodeProtobufMessage(const google::protobuf::Message& message,
                                  std::string* out) {
    return message.SerializeToString(out);
}

template <typename Message>
inline bool DecodeProtobufMessage(const std::string& encode, Message* message) {
    return message->ParseFromString(encode);
}

}  // namespace codec
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_CODEC_CODEC_H_

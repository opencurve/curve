/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Date: Tue Apr 25 20:09:45 CST 2023
 * Author: lixiaocui
 */

#ifndef CURVEFS_TEST_METASERVER_SPACE_MOCK_INODE_VOLUME_SPACE_DEALLOCATE_H_
#define CURVEFS_TEST_METASERVER_SPACE_MOCK_INODE_VOLUME_SPACE_DEALLOCATE_H_

#include <gmock/gmock.h>

#include "curvefs/src/metaserver/space/inode_volume_space_deallocate.h"

namespace curvefs {
namespace metaserver {

class MockInodeVolumeSpaceDeallocate : public InodeVolumeSpaceDeallocate {
 public:
    MockInodeVolumeSpaceDeallocate()
        : InodeVolumeSpaceDeallocate(InodeVolumeSpaceDeallocateOption{},
                                     nullptr, 0, 0) {}
    MOCK_METHOD0(CalDeallocatableSpace, void());
};

}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_TEST_METASERVER_SPACE_MOCK_INODE_VOLUME_SPACE_DEALLOCATE_H_

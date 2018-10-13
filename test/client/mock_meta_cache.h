/*
 * Project: curve
 * Created Date: 18-10-7
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CLIENT_MOCK_META_CACHE_H
#define CURVE_CLIENT_MOCK_META_CACHE_H

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/client/client_common.h"
#include "src/client/metacache.h"
#include "src/client/session.h"

namespace curve {
namespace client {

using ::testing::_;
using ::testing::Invoke;

class FakeMetaCache : public MetaCache {
 public:
    FakeMetaCache() : MetaCache(new Session()) {}

    int GetLeader(LogicPoolID logicPoolId,
                  CopysetID copysetId,
                  ChunkServerID *serverId,
                  butil::EndPoint *serverAddr,
                  bool refresh = false) {
        *serverId = 10000;
        butil::str2endpoint("127.0.0.1:8200", serverAddr);
        return 0;
    }
    int UpdateLeader(LogicPoolID logicPoolId,
                     CopysetID copysetId,
                     ChunkServerID *leaderId,
                     const butil::EndPoint &leaderAddr) {
        return 0;
    }
};

class MockMetaCache : public MetaCache {
 public:
    MockMetaCache() : MetaCache(new Session()) {}

    MOCK_METHOD5(GetLeader, int(LogicPoolID, CopysetID, ChunkServerID*,
                                butil::EndPoint *, bool));
    MOCK_METHOD4(UpdateLeader, int(LogicPoolID, CopysetID, ChunkServerID*,
                                   const butil::EndPoint &));

    void DelegateToFake() {
        ON_CALL(*this, GetLeader(_, _, _, _, _))
            .WillByDefault(Invoke(&fakeMetaCache_, &FakeMetaCache::GetLeader));
        ON_CALL(*this, UpdateLeader(_, _, _, _))
            .WillByDefault(Invoke(&fakeMetaCache_,
                                  &FakeMetaCache::UpdateLeader));
    }

 private:
    FakeMetaCache fakeMetaCache_;
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_MOCK_META_CACHE_H

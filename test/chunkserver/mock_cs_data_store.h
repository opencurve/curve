/*
 * Project: curve
 * Created Date: 18-11-14
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_MOCK_CS_DATA_STORE_H
#define CURVE_MOCK_CS_DATA_STORE_H

#include <string>

#include "src/chunkserver/chunkserverStorage/chunkserver_datastore.h"

namespace curve {
namespace chunkserver {

class FakeCSDataStore : public CSDataStore {
 public:
    FakeCSDataStore() : CSDataStore() {
    }

    bool ReadChunk(ChunkID id,
                   char *buf,
                   off_t offset,
                   size_t *length) {
        return false;
    }
    bool WriteChunk(ChunkID id,
                    const char *buf,
                    off_t offset,
                    size_t length) {
        return false;
    }
};

class MockCSDataStore : public CSDataStore {
 public:
    MockCSDataStore() : CSDataStore() {
    }

    bool Initialize(std::shared_ptr<CSSfsAdaptor> fsadaptor,
                    std::string copysetdir) {
        return false;
    }
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_MOCK_CS_DATA_STORE_H

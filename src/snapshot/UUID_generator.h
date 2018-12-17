/*
 * Project: curve
 * Created Date: Mon Dec 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_UUID_GENERATOR_H_
#define CURVE_SRC_SNAPSHOT_UUID_GENERATOR_H_
extern "C" {
#include <uuid/uuid.h>
void uuid_generate(uuid_t out);
void uuid_generate_random(uuid_t out);
void uuid_generate_time(uuid_t out);
int uuid_generate_time_safe(uuid_t out);
}
#include <string>
#define BUFF_LEN 36
namespace curve {
namespace snapshotserver {

class UUIDGenerator {
 public:
    UUIDGenerator() {}

    std::string GenerateUUID() {
        uuid_t out;
        char buf[BUFF_LEN];
        uuid_generate(out);
        uuid_unparse_lower(out, buf);
        std::string str(&buf[0], BUFF_LEN);
        return str;
    }
    std::string GenerateUUIDRandom() {
        uuid_t out;
        char buf[BUFF_LEN];
        uuid_generate_time(out);
        uuid_unparse_lower(out, buf);
        std::string str(&buf[0], BUFF_LEN);
        return str;
    }
    std::string GenerateUUIDTime() {
        uuid_t out;
        char buf[BUFF_LEN];
        uuid_generate_random(out);
        uuid_unparse_lower(out, buf);
        std::string str(&buf[0], BUFF_LEN);
        return str;
    }
    std::string GenerateUUIDTimeSafe() {
        uuid_t out;
        char buf[BUFF_LEN];
        uuid_generate_time_safe(out);
        uuid_unparse_lower(out, buf);
        std::string str(&buf[0], BUFF_LEN);
        return str;
    }
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_UUID_GENERATOR_H_

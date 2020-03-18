/*
 * Project: curve
 * Created Date: Mon Dec 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_COMMON_UUID_H_
#define SRC_COMMON_UUID_H_
extern "C" {
#include <uuid/uuid.h>
void uuid_generate(uuid_t out);
void uuid_generate_random(uuid_t out);
void uuid_generate_time(uuid_t out);
// 指明由uuid_generate_time生成的uuid是否使用了时间同步机制，不进行封装。
int uuid_generate_time_safe(uuid_t out);
}
#include <string>
#define BUFF_LEN 36
namespace curve {
namespace common {
// 生成uuid的生成器
class UUIDGenerator {
 public:
    UUIDGenerator() {}

    /**
     *  @brief 生成uuid，优先采用的算法
     *  如果存在一个高质量的随机数生成器(/dev/urandom），
     *  UUID将基于其生成的随机数产生。
     *  备用算法：在高质量的随机数生成器不可用的情况下，如果可以获取到MAC地址，
     *  则将利用由随机数生成器产生的随机数、当前时间、MAC地址生成UUID。
     *  @param :
     *  @return 生成的uuid
     */
    std::string GenerateUUID() {
        uuid_t out;
        char buf[BUFF_LEN];
        uuid_generate(out);
        uuid_unparse_lower(out, buf);
        std::string str(&buf[0], BUFF_LEN);
        return str;
    }

    /**
     *  @brief 生成uuid
     *  使用全局时钟、MAC地址。有MAC地址泄露风险。为了保证唯一性还使用的时间同步机制，
     *  如果，时间同步机制不可用，多台机器上生成的uuid可能会重复。
     *  @param :
     *  @return 生成的uuid
     */
    std::string GenerateUUIDTime() {
        uuid_t out;
        char buf[BUFF_LEN];
        uuid_generate_time(out);
        uuid_unparse_lower(out, buf);
        std::string str(&buf[0], BUFF_LEN);
        return str;
    }

    /**
     *  @brief 生成uuid
     *  强制完全使用随机数，优先使用（/dev/urandom），备用（伪随机数生成器）。
     *  在使用伪随机数生成器的情况下，uuid有重复的风险。
     *  @return 生成的uuid
     */
    std::string GenerateUUIDRandom() {
        uuid_t out;
        char buf[BUFF_LEN];
        uuid_generate_random(out);
        uuid_unparse_lower(out, buf);
        std::string str(&buf[0], BUFF_LEN);
        return str;
    }
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_UUID_H_

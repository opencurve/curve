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
 * Created Date: Mon Dec 17 2018
 * Author: xuchaojie
 */

#ifndef SRC_COMMON_UUID_H_
#define SRC_COMMON_UUID_H_
extern "C" {
#include <uuid/uuid.h>
void uuid_generate(uuid_t out);
void uuid_generate_random(uuid_t out);
void uuid_generate_time(uuid_t out);
//Indicate by uuid_ Generate_ Is the uuid generated by time using a time synchronization mechanism and not encapsulated.

int uuid_generate_time_safe(uuid_t out);
}
#include <string>
#define BUFF_LEN 36
namespace curve {
namespace common {
//Generator for generating uuids

class UUIDGenerator {
 public:
    UUIDGenerator() {}

    /**
     * @brief generates uuid, priority algorithm to use
     *If there is a high-quality random number generator (/dev/urandom),
     *UUID will be generated based on the random number it generates.
     *Backup algorithm: If a high-quality random number generator is not available, if the MAC address can be obtained,
     *The UUID will be generated using the random number generated by the random number generator, current time, and MAC address.
     * @param:
     *Uuid generated by @ return
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
     *Generate uuid for  @brief
     *Use global clock and MAC address. There is a risk of MAC address leakage. To ensure uniqueness, a time synchronization mechanism is also used,
     *If the time synchronization mechanism is not available, the uuids generated on multiple machines may be duplicated.
     * @param:
     *Uuid generated by @ return
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
     *Generate uuid for  @brief
     *Force full use of random numbers, prioritize (/dev/urandom), and backup (pseudo random number generator).
     *When using pseudo random number generators, there is a risk of duplication in uuids.
     *Uuid generated by @ return
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

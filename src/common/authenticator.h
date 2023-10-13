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
 * File Created: Monday, 1st April 2019 5:15:20 pm
 * Author: tongguangxun
 */
#ifndef SRC_COMMON_AUTHENTICATOR_H_
#define SRC_COMMON_AUTHENTICATOR_H_

#include <string>

namespace curve {
namespace common {

class Authenticator {
 public:
    /**
     * bref: Get the string to be signed
     * @param: date, current time
     * @param: owner, file owner
     * @return: Returns the string that needs to be encrypted
     */
    static std::string GetString2Signature(uint64_t date,
                                           const std::string& owner);

    /**
     * bref: Calculate signature for string
     * @param: String2Signature, a string that requires signature calculation
     * @param: secretKey, which is the calculated secret key
     * @return: Returns the string that needs to be signed
     */
    static std::string CalcString2Signature(const std::string& String2Signature,
                                            const std::string& secretKey);

 private:
    static int HMacSha256(const void* key, int key_size, const void* data,
                          int data_size, void* digest);

    static std::string Base64(const unsigned char* src, size_t sz);
};
}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_AUTHENTICATOR_H_

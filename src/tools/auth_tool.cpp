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
 * Created Date: 2023-06-27
 * Author: wanghai (SeanHai)
 */

#include <iostream>
#include <memory>
#include <string>
#include "src/tools/auth_tool.h"
#include "src/common/authenticator.h"
#include "src/tools/curve_tool_define.h"
# include "proto/auth.pb.h"
#include "src/tools/mds_client.h"

DEFINE_string(user, "", "the user add");
DEFINE_string(role, "", "role");
DEFINE_string(key, "", "key");
DEFINE_string(authkey, "", "authkey");
DEFINE_string(caps, "*", "caps");

namespace curve {
namespace tool {

bool AuthTool::SupportCommand(const std::string& command) {
    return (command == kAuthKeyAdd || command == kAuthKeyDelete ||
            command == kAuthKeyGet || command == kAuthKeyUpdate);
}

void AuthTool::PrintHelp(const std::string &cmd) {
    std::cout << "Example :" << std::endl;
    std::cout << "curve_ops_tool " << cmd;
    if (cmd == kAuthKeyAdd) {
        std::cout << " -user tools -role client|service -key 1234567890abcdef(16 byte) -authkey auth_service_key" << std::endl;  // NOLINT
    } else if (cmd == kAuthKeyDelete) {
        std::cout << " -user tools -authkey auth_service_key" << std::endl;
    } else if (cmd == kAuthKeyGet) {
        std::cout << " -user tools -authkey auth_service_key" << std::endl;
    } else if (cmd == kAuthKeyUpdate) {
        std::cout << " -user tools -key newKey -authkey auth_service_key" << std::endl;  // NOLINT
    }
}

int AuthTool::AddKey() {
    if (FLAGS_user.empty() || FLAGS_role.empty() || FLAGS_key.empty() ||
        FLAGS_authkey.empty() || FLAGS_caps.empty() ||
        (FLAGS_role != "client" && FLAGS_role != "service")) {
        PrintHelp(kAuthKeyAdd);
        return -1;
    }
    curve::mds::auth::Key key;
    key.set_id(FLAGS_user);
    key.set_key(FLAGS_key);
    key.set_caps(FLAGS_caps);
    key.set_role(FLAGS_role == "client" ?
        curve::mds::auth::RoleType::ROLE_CLIENT :
        curve::mds::auth::RoleType::ROLE_SERVICE);
    std::string keyStr;
    if (!key.SerializeToString(&keyStr)) {
        std::cout << "serialize key failed" << std::endl;
        return -1;
    }
    std::string encKeyStr;
    auto enc = curve::common::Encryptor::AESEncrypt(
        FLAGS_authkey, curve::common::ZEROIV, keyStr, &encKeyStr);
    if (enc != 0) {
        std::cout << "encrypt key failed" << std::endl;
        return -1;
    }
    return mdsClient_->AddKey(encKeyStr);
}

int AuthTool::DeleteKey() {
    if (FLAGS_user.empty() || FLAGS_authkey.empty()) {
        PrintHelp(kAuthKeyDelete);
        return -1;
    }
    std::string encUser;
    auto enc = curve::common::Encryptor::AESEncrypt(
        FLAGS_authkey, curve::common::ZEROIV, FLAGS_user, &encUser);
    if (enc != 0) {
        std::cout << "encrypt user failed" << std::endl;
        return -1;
    }
    return mdsClient_->DelKey(encUser);
}

int GetKeyFromMds(std::shared_ptr<MDSClient> mdsClient,
    const std::string &user,
    const std::string &authkey,
    curve::mds::auth::Key *key) {
    std::string encUser;
    auto enc = curve::common::Encryptor::AESEncrypt(
        authkey, curve::common::ZEROIV, user, &encUser);
    if (enc != 0) {
        std::cout << "encrypt user failed" << std::endl;
        return -1;
    }
    std::string keyStr;
    if (mdsClient->GetKey(encUser, &keyStr) != 0) {
        std::cout << "get key failed" << std::endl;
        return -1;
    }
    std::string decKeyStr;
    auto dec = curve::common::Encryptor::AESDecrypt(
        authkey, curve::common::ZEROIV, keyStr, &decKeyStr);
    if (dec != 0) {
        std::cout << "decrypt key failed" << std::endl;
        return -1;
    }
    if (!key->ParseFromString(decKeyStr)) {
        std::cout << "parse key failed" << std::endl;
        return -1;
    }
    return 0;
}

int AuthTool::GetKey() {
    if (FLAGS_user.empty() || FLAGS_authkey.empty()) {
        PrintHelp(kAuthKeyGet);
        return -1;
    }
    curve::mds::auth::Key key;
    auto isGet = GetKeyFromMds(mdsClient_, FLAGS_user, FLAGS_authkey, &key);
    if (isGet != 0) {
        return -1;
    }
    std::cout << "keyInfo: " << key.ShortDebugString() <<std::endl;
    return 0;
}

int AuthTool::UpdateKey() {
    if (FLAGS_user.empty() || FLAGS_authkey.empty() || FLAGS_key.empty()) {
        PrintHelp(kAuthKeyUpdate);
        return -1;
    }
    curve::mds::auth::Key key;
    auto isGet = GetKeyFromMds(mdsClient_, FLAGS_user, FLAGS_authkey, &key);
    if (isGet != 0) {
        return -1;
    }
    key.set_key(FLAGS_key);
    std::string keyStr;
    if (!key.SerializeToString(&keyStr)) {
        std::cout << "serialize key failed" << std::endl;
        return -1;
    }
    std::string encKeyStr;
    if (curve::common::Encryptor::AESEncrypt(
        FLAGS_authkey, curve::common::ZEROIV, keyStr, &encKeyStr) != 0) {
        std::cout << "encrypt key failed" << std::endl;
        return -1;
    }
    return mdsClient_->UpdateKey(encKeyStr);
}

int AuthTool::Init() {
    if (mdsClient_->Init(FLAGS_mdsAddr) != 0) {
        std::cout << "init mds client failed" << std::endl;
        return -1;
    }
    return 0;
}

int AuthTool::RunCommand(const std::string& command) {
    if (Init() != 0) {
        std::cout << "init auth tool failed" << std::endl;
        return -1;
    }

    if (command == kAuthKeyAdd) {
        return AddKey();
    } else if (command == kAuthKeyDelete) {
        return DeleteKey();
    } else if (command == kAuthKeyGet) {
        return GetKey();
    } else if (command == kAuthKeyUpdate) {
        return UpdateKey();
    } else {
        std::cout << "command not support" << std::endl;
        return -1;
    }
}

}  // namespace tool
}  // namespace curve

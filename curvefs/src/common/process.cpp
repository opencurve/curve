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

/**
 * Project: Curve
 * Created Date: 2021-07-23
 * Author: Jingli Chen (Wine93)
 */

#include <cstring>

#include "curvefs/src/common/process.h"

extern char** environ;

namespace curvefs {
namespace common {

char** Process::OsArgv_ = nullptr;
char* Process::OsArgvLast_ = nullptr;

pid_t Process::SpawnProcess(ProcFunc proc) {
    pid_t pid = fork();

    switch (pid) {
        case -1:
            return -1;
        case 0:
            proc();
            break;
        default:
            break;
    }

    return pid;
}

void Process::InitSetProcTitle(int argc, char* const* argv) {
    size_t size = 0;
    for (auto i = 0; environ[i]; i++) {
        size += strlen(environ[i]) + 1;
    }

    OsArgv_ = (char**) argv;  // NOLINT
    OsArgvLast_ = OsArgv_[0];
    for (auto i = 0; OsArgv_[i]; i++) {
        if (OsArgvLast_ == OsArgv_[i]) {
            OsArgvLast_ = OsArgvLast_ + strlen(OsArgv_[i]) + 1;
        }
    }

    char* p = new (std::nothrow) char[size];
    for (auto i = 0; environ[i]; i++) {
        if (OsArgvLast_ == environ[i]) {
            size = strlen(environ[i]) + 1;
            OsArgvLast_ = environ[i] + size;

            strncpy(p, environ[i], size);
            environ[i] = p;
            p += size;
        }
    }

    // Ensure the last char is '\0' when title too long
    OsArgvLast_--;
}

void Process::SetProcTitle(const std::string& title) {
    OsArgv_[1] = NULL;
    strncpy(OsArgv_[0], title.c_str(), OsArgvLast_ - OsArgv_[0]);
}

bool Process::InitSignals(const std::vector<Signal>& signals) {
    struct sigaction sa;

    for (const auto& signal : signals) {
        memset(&sa, 0, sizeof(struct sigaction));
        if (signal.handler) {
            sa.sa_sigaction = signal.handler;
            sa.sa_flags = SA_SIGINFO;
        } else {
            sa.sa_handler = SIG_IGN;
        }

        sigemptyset(&sa.sa_mask);
        if (sigaction(signal.signo, &sa, NULL) == -1) {
            return false;
        }
    }

    return true;
}

};  // namespace common
};  // namespace curvefs

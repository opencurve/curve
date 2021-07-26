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

#ifndef CURVEFS_SRC_COMMON_PROCESS_H_
#define CURVEFS_SRC_COMMON_PROCESS_H_

#include <unistd.h>
#include <signal.h>

#include <string>
#include <vector>
#include <functional>

namespace curvefs {
namespace common {

using ProcFunc = std::function<void()>;
using SignalHandler = std::function<void(int, siginfo_t, void*)>;

struct Signal {
    int signo;
    void (*handler)(int signo, siginfo_t* siginfo, void* ucontext);
};

class Process {
 public:
    static pid_t SpawnProcess(ProcFunc proc);
    static void InitSetProcTitle(int argc, char* const* argv);
    static void SetProcTitle(const std::string& title);
    static bool InitSignals(const std::vector<Signal>& signals);

 private:
    static char** OsArgv_;
    static char* OsArgvLast_;
};

};  // namespace common
};  // namespace curvefs

#endif  // CURVEFS_SRC_COMMON_PROCESS_H_

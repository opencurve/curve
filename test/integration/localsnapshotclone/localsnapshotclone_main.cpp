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
 * Created Date: 2023-06-27
 * Author: xuchaojie
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>


int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_log_dir = "./runlog";
    FLAGS_v = 9;
    google::InitGoogleLogging(argv[0]);

    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}

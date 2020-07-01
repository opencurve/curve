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
 * Created Date: Monday May 13th 2019
 * Author: hzsunjianliang
 */

/* libfiu - Fault Injection in Userspace
 *
 * This header, part of libfiu, is meant to be included in your project to
 * avoid having libfiu as a mandatory build-time dependency.
 *
 * You can add it to your project, and #include it instead of fiu.h.
 * The real fiu.h will be used only when FIU_ENABLE is defined.
 *
 * This header, as the rest of libfiu, is in the public domain.
 *
 * You can find more information about libfiu at
 * http://blitiri.com.ar/p/libfiu.
 */

#ifndef TEST_FAILPOINT_FIU_LOCAL_H_
#define TEST_FAILPOINT_FIU_LOCAL_H_

/* Only define the stubs when fiu is disabled, otherwise use the real fiu.h
 * header */
#ifndef FIU_ENABLE

#define fiu_init(flags) 0
#define fiu_fail(name) 0
#define fiu_failinfo() NULL
#define fiu_do_on(name, action)
#define fiu_exit_on(name)
#define fiu_return_on(name, retval)

#else

#include <fiu.h>

#endif  // FIU_ENABLE
#endif  // TEST_FAILPOINT_FIU_LOCAL_H_

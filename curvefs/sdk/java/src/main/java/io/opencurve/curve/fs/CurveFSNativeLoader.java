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
 * Project: Curve
 * Created Date: 2023-07-12
 * Author: Jingli Chen (Wine93)
 */

package io.opencurve.curve.fs;

public class CurveFSNativeLoader {
    private static boolean initialized = false;
    private static final CurveFSNativeLoader instance = new CurveFSNativeLoader();

    private static final String LIBRARY_FILE = "/usr/lib/libcurvefs_jni.so";

    private CurveFSNativeLoader() {}

    public static CurveFSNativeLoader getInstance() {
        return instance;
    }
    public synchronized void loadLibrary() {
        if (initialized) {
            return;
        }

        System.load(LIBRARY_FILE);
        initialized = true;
    }
}

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

public class CurveFS {
    private long cInstancePtr;

    private static native long nativeCurveFScreate();
    private static native int nativeCurveFSMount(long cInstancePtr);
    private static native int nativeCurveFSMkdir(long cInstancePtr, String path, int mode);
    private static native int nativeCurveFSRmdir(long cInstancePtr, String path);

    static {
        CurveFSNativeLoader.getInstance().loadLibrary();
    }

    public CurveFS() {
        cInstancePtr = nativeCurveFScreate();
    }

    public void mount() {
        nativeCurveFSMount(cInstancePtr);
    }

    public void mkdir(String path, int mode) {
        nativeCurveFSMkdir(cInstancePtr, path, mode);
    }

    public void rmdir(String path) {
        nativeCurveFSRmdir(cInstancePtr, path);
    }
};

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
 * Created Date: 2023-07-07
 * Author: Jingli Chen (Wine93)
 */

package io.opencurve.curve.fs.libfs;

import java.net.URL;
import java.net.URLConnection;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class CurveFSNativeLoader {
    private static boolean initialized = false;
    private static final CurveFSNativeLoader instance = new CurveFSNativeLoader();

    private static final String TMP_DIR = "/tmp";
    private static final String RESOURCE_NAME = "libcurvefs_jni.so";

    private CurveFSNativeLoader() {}

    public static CurveFSNativeLoader getInstance() {
        return instance;
    }

    public long getJarModifiedTime() throws IOException {
        URL location = CurveFSNativeLoader.class.getProtectionDomain().getCodeSource().getLocation();
        URLConnection conn = location.openConnection();
        return conn.getLastModified();
    }

    public synchronized void loadLibrary() throws IOException {
        if (initialized) {
            return;
        }

        long jarModifiedTime = getJarModifiedTime();
        File libFile = new File(TMP_DIR, RESOURCE_NAME);
        if (libFile.exists() && libFile.lastModified() >= jarModifiedTime) {
            System.load(libFile.getPath());
            initialized = true;
            return;
        }

        InputStream reader = CurveFSNativeLoader.class.getResourceAsStream("/" + RESOURCE_NAME);
        if (reader == null) {
            throw new IOException("Cannot get resource " + RESOURCE_NAME + " from Jar file.");
        }

        FileOutputStream writer = new FileOutputStream(libFile);
        byte[] buffer = new byte[128 << 10];
        int nbytes = 0;
        while ((nbytes = reader.read(buffer)) > 0) {
            writer.write(buffer, 0, nbytes);
        }
        libFile.setLastModified(jarModifiedTime);
        reader.close();
        writer.close();

        System.load(libFile.getPath());
        initialized = true;
    }
}


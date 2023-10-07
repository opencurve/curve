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

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.utils.IOUtils;

public class CurveFSNativeLoader {
    boolean initialized = false;
    private static final CurveFSNativeLoader instance = new CurveFSNativeLoader();

    private static final String TMP_DIR = "/tmp";
    private static final String CURVEFS_LIBRARY_PATH = "/tmp/libcurvefs";
    private static final String RESOURCE_TAR_NAME = "libcurvefs.tar";
    private static final String JNI_LIBRARY_NAME = "libcurvefs_jni.so";

    private CurveFSNativeLoader() {}

    public static CurveFSNativeLoader getInstance() {
        return instance;
    }

    public long getJarModifiedTime() throws IOException {
        URL location = CurveFSNativeLoader.class.getProtectionDomain().getCodeSource().getLocation();
        URLConnection conn = location.openConnection();
        return conn.getLastModified();
    }

    public void descompress(InputStream in, String dest) throws IOException {
        File dir = new File(dest);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        ArchiveEntry entry;
        TarArchiveInputStream reader = new TarArchiveInputStream(in);
        while ((entry = reader.getNextTarEntry()) != null) {
            if (entry.isDirectory()) {
                continue;
            }

            String path = TMP_DIR + File.separator + entry.getName();
            File file = new File(path);
            IOUtils.copy(reader, new FileOutputStream(file));
        }
        reader.close();
    }

    public void loadJniLibrary() throws IOException {
        File libFile = new File(CURVEFS_LIBRARY_PATH, JNI_LIBRARY_NAME);
        System.load(libFile.getAbsolutePath());
    }

    public synchronized void loadLibrary() throws IOException {
        if (initialized) {
            return;
        }

        long jarModifiedTime = getJarModifiedTime();
        File libDir = new File(CURVEFS_LIBRARY_PATH);
        if (libDir.exists() && libDir.lastModified() == jarModifiedTime) {
            loadJniLibrary();
            initialized = true;
            return;
        }

        InputStream reader = CurveFSNativeLoader.class.getResourceAsStream("/" + RESOURCE_TAR_NAME);
        if (reader == null) {
            throw new IOException("Cannot get resource " + RESOURCE_TAR_NAME + " from Jar file.");
        }
        descompress(reader, CURVEFS_LIBRARY_PATH);
        reader.close();

        libDir.setLastModified(jarModifiedTime);
        loadJniLibrary();
        initialized = true;
    }
}

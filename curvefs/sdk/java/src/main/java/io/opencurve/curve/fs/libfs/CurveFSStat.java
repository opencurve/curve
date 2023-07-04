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

public class CurveFSStat {
    public boolean is_file;       /* S_ISREG */
    public boolean is_directory;  /* S_ISDIR */
    public boolean is_symlink;    /* S_ISLNK */

    public int mode;
    public int uid;
    public int gid;
    public long size;
    public long blksize;
    public long blocks;
    public long a_time;
    public long m_time;

    public boolean isFile() {
        return is_file;
    }

    public boolean isDir() {
        return is_directory;
    }

    public boolean isSymlink() {
        return is_symlink;
    }
}

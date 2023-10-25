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
 * Created Date: 2023-08-01
 * Author: Xianfei Cao (caoxianfei1)
 */

package io.opencurve.curve.fs.hadoop.permission;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class Permission {
    private static User user = null;
    private static Group group = null;

    public Permission() {
        user = new User();
        group = new Group();
    }

    public void initialize(Configuration conf) throws IOException {
        user.initialize(null);
        group.initialize(null);
    }

    public int getCurrentUid() {
        return user.getCurrentUid();
    }

    public int getUid(String username) {
        return user.getUid(username);
    }

    public String getUsername(int uid) {
        return user.getUsername(uid);
    }

    public int[] getCurrentGids() {
        return group.getCurrentGids();
    }

    public int getGid(String groupname) {
        return group.getGid(groupname);
    }

    public String getGroupname(int gid) {
        return group.getGroupname(gid);
    }
}

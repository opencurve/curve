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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.HashMap;
import java.io.IOException;
import java.util.List;

public class User {
    private HashMap<String, Integer> usernames;
    private HashMap<Integer, String> userIDs;
    private UserGroupInformation ugi;

    private static final String SUPER_USERNAME = "hdfs";
    private static final int SUPER_UID = 0;

    public User() {
        usernames = new HashMap<String, Integer>();
        userIDs = new HashMap<Integer, String>();
    }

    private int finger(String groupname) {
        return Math.abs(groupname.hashCode());
    }

    private void addUser(String username, int uid) {
        usernames.put(username, uid);
        userIDs.put(uid, username);
    }

    private void loadUserFromSystem() throws IOException {
        List<Entry> users = Helper.getAllUsers();
        for (Entry user : users) {
            if (user.id == 0) {
                user.id = finger(user.name);
            }
            addUser(user.name, user.id);
        }
        addUser(SUPER_USERNAME, SUPER_UID);
    }

    private void loadUserFromFile() throws IOException {
        // TODO: implement it
    }

    public void initialize(Path path) throws IOException {
        this.ugi = UserGroupInformation.getCurrentUser();
        if (path != null) {
            loadUserFromFile();
        } else {
            loadUserFromSystem();
        }
    }

    public int getCurrentUid() {
        String username = ugi.getShortUserName();
        return getUid(username);
    }

    public int getUid(String username) {
        Integer gid = usernames.get(username);
        if (null == gid) {
            gid = finger(username);
            addUser(username, gid);
        }
        return gid;
    }

    public String getUsername(int uid) {
        String username = userIDs.get(uid);
        if (null == username || username.isEmpty()) {
            return String.valueOf(uid);
        }
        return username;
    }
}

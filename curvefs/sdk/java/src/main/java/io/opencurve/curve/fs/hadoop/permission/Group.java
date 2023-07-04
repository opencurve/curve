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

public class Group {
    private HashMap<String, Integer> groupnames;
    private HashMap<Integer, String> groupIDs;
    private UserGroupInformation ugi;

    private static final String SUPER_GROUPNAME = "supergroup";
    private static final int SUPER_GID = 0;

    public Group() {
        groupnames = new HashMap<String, Integer>();
        groupIDs = new HashMap<Integer, String>();
    }

    private int finger(String groupname) {
        return Math.abs(groupname.hashCode());
    }

    private void addGroup(String groupname, int gid) {
        groupnames.put(groupname, gid);
        groupIDs.put(gid, groupname);
    }

    private void loadGroupFromSystem() throws IOException {
        List<Entry> groups = Helper.getAllGroups();
        for (Entry group : groups) {
            if (group.id == 0) {
                group.id = finger(group.name);
            }
            addGroup(group.name, group.id);
        }
        addGroup(SUPER_GROUPNAME, SUPER_GID);
    }

    private void loadGroupFromFile() throws IOException {
        // implement it
    }

    public void initialize(Path path) throws IOException {
        this.ugi = UserGroupInformation.getCurrentUser();
        if (path != null) {
            loadGroupFromFile();
        } else {
           loadGroupFromSystem();
        }
    }

    public int[] getCurrentGids() {
        String[] groups = {"nogroup"};
        if (ugi.getGroupNames().length > 0) {
            groups = ugi.getGroupNames();
        }

        int[] gids = new int[groups.length];
        for (int i = 0; i < groups.length; i++) {
            gids[i] = getGid(groups[i]);
        }
        return gids;
    }

    public int getGid(String groupname) {
        Integer gid = groupnames.get(groupname);
        if (null == gid) {
            gid = finger(groupname);
            addGroup(groupname, gid);
        }
        return gid;
    }

    public String getGroupname(int gid) {
        String groupname = groupIDs.get(gid);
        if (null == groupname || groupname.isEmpty()) {
            return String.valueOf(gid);
        }
        return groupname;
    }
}

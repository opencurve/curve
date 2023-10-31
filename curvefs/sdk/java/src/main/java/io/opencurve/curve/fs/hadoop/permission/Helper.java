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

//import com.sun.jna.Library;
//import com.sun.jna.Native;
//import com.sun.jna.Pointer;
//import com.sun.jna.Structure;
//import io.opencurve.curve.fs.hadoop.permission.Helper.CLibrary.passwd;
//import io.opencurve.curve.fs.hadoop.permission.Helper.CLibrary.group;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

class Entry {
    String name;
    int id;

    public Entry(String name, int id) {
        this.name = name;
        this.id = id;
    }
}

public class Helper {
    /*
    public interface CLibrary extends Library {
        CLibrary INSTANCE = (CLibrary)Native.loadLibrary("c", CLibrary.class);
        passwd getpwent();
        void endpwent();
        group getgrent();
        void endgrent();

        public static class passwd extends Structure {
            public String pw_name;
            public String pw_passwd;
            public int pw_uid;
            public int pw_gid;
            public String pw_gecos;
            public String pw_dir;
            public String pw_shell;

            protected List<String> getFieldOrder() {
                return Arrays.asList(new String[] {
                    "pw_name", "pw_passwd", "pw_uid", "pw_gid", "pw_gecos", "pw_dir", "pw_shell",
                });
            }
        }

        public static class group extends Structure {
            public String gr_name;
            public String gr_passwd;
            public int gr_gid;
            public Pointer gr_mem;

            protected List<String> getFieldOrder() {
                return Arrays.asList(new String[] {
                    "gr_name", "gr_passwd", "gr_gid", "gr_mem",
                });
            }
        }
    }
    */

    public static List<Entry> getAllUsers() {
        /*
        CLibrary lib = CLibrary.INSTANCE;
        List<Entry> users = new ArrayList<Entry>();
        passwd usr = new passwd();
        while ((usr = lib.getpwent()) != null) {
            users.add(new Entry(usr.pw_name, usr.pw_uid));
        }
        lib.endpwent();
        return users;
        */

        List<Entry> users = new ArrayList<Entry>();
        return users;
    }

    public static List<Entry> getAllGroups() {
        /*
        CLibrary lib = CLibrary.INSTANCE;
        List<Entry> groups = new ArrayList<Entry>();
        group grp = new group();
        while ((grp = lib.getgrent()) != null) {
            groups.add(new Entry(grp.gr_name, grp.gr_gid));
        }
        lib.endgrent();
        return groups;
        */
        List<Entry> groups = new ArrayList<Entry>();
        return groups;
    }
}

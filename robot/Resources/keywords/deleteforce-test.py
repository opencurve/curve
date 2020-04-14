#!/usr/bin/env python
# -*- coding: utf8 -*-
#!/usr/bin/env python
# -*- coding: utf8 -*-
import curvefs
import time
FILENAME = "/cyh"

curvefs.Init("/etc/curve/client.conf")

user_info_t = curvefs.UserInfo_t()
user_info_t.owner = "test"
user_info_t.password = "root_password"
curvefs.Create(FILENAME, user_info_t, 10*1024*1024*1024)
fd = curvefs.Open(FILENAME, user_info_t)
#print fd
for i in range(0, 10):
    curvefs.Write(fd,"aaaa"*1024, i*1024*1024*1024, 4*1024)

for i in range(0, 10):
    curvefs.Read(fd, "", i*1024*1024*1024, 4*1024)
            
user1 = curvefs.UserInfo_t()
user1.owner = "root"
user1.password = "root_password"
dirs = curvefs.Listdir("/", user1)
#for dir in dirs:
    #print dir
curvefs.Close(fd) 
curvefs.Unlink(FILENAME, user1)

recycleBin_dirs = curvefs.Listdir("/RecycleBin", user1)
l = len(FILENAME)
for i in recycleBin_dirs:
    if  FILENAME.strip("/") in i:
        id = i.split("-",1)[1]
        print "file id is %s"%id
curvefs.DeleteForce("/RecycleBin"+FILENAME+"-"+id, user1)
delete_success = 0
for i in range(1,10):
    recycleBin_dirs = curvefs.Listdir("/RecycleBin", user1)
    if "cyh-"+ id not in   recycleBin_dirs:
        delete_success = 1
        print "file id is %s"%id
    else:
        print "delete file success in time %d"%i
        time.sleep(1)
assert delete_success == 1,"deleteforce %s timeout 10s"%recycleBin_dirs

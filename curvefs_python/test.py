#!/usr/bin/env python
# coding=utf-8


import curvefs

ret = curvefs.Init("./client.conf");
print ret

user = curvefs.UserInfo_t()
user.owner = "zjm"

curvefs.Mkdir("/zjm", user)

curvefs.Create("/zjm/test1", user, 10*1024*1024*1024)
curvefs.Create("/zjm/test2", user, 10*1024*1024*1024)
curvefs.Create("/zjm/test3", user, 10*1024*1024*1024)

dir = curvefs.Listdir("/zjm", user)
for i in dir:
    print i

curvefs.UnInit()

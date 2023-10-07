
CurveFS VFS Design
===

Overview
---

```

```

Permission
---

`uid/gid` AND `euid/egid` AND `suid/sgid`

```C++
euid = uid
egid = gid

if (suid != NULL) {
    euid = suid;
}

if (sgid != NULL) {
    egid = sgid;
}
```

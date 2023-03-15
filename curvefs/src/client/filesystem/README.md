CurveFS Client Metadata Cache Design
===

```
+----------------+
+  Kernel Cache  |
+----------------+
    |       ^
(1) |       | (4)
    v       |
+----------------+
+  CurveFS Fuse  |
+----------------+
    |       ^
(2) |       | (3)
    v       |
+----------------+
+   MetaServer   |
+----------------+
```

(1) kernel cache timeout
---

* requests are always sent to fuse layer, except `lookup` and `getattr` request which affected by caching.
* if entry or attribute cache timeout, vfs layer will launch request to fuse layer.

(2) revalidate cache
---

* for `open` and `opendir` request, fuse layer should revalidate cache by comparing mtime, if modified, fuse layer should:
    * `open`: return **ESTALE** to trigger vfs layer to invoke the `open` again with ignoring cache.
    * `opendir`: drop all directory cache.
* others, proxy request to metaserver directly.


(3) retrive fresh metadata
---

* for `readdir` request, fuse layer should cache direcoty entries and their attributes.
* others, no caching.

(4) reply with timeout
---

* reply entry or attribute to vfs layer with corresponding cache timeout.
* four type timeouts provided: `entryTimeout`, `dirEntryTimeout`, `attrTimeout`, `dirAtttTimeout`.
* fuse layer should remeber the `mtime` of attribute while reply it to vfs layer.

Cache Layer Level
===

```
+----------------------------------+
|           Kernel Cache           |  (L1 Cache)
+----------------------------------+
                 ^
                 |
                 |
+----------------------------------+
|          Open File Cache         |  (L2 Cache)
|      (only length and chunk)     |
+----------------------------------+
                 ^
                 |
                 |
+----------------------------------+
|          Directory Cache         |  (L3 Cache)
+----------------------------------+
     ^            ^            ^
     |            |            |
     |            |            |
+---------+  +---------+  +--------+
| getattr |  + setattr |  + lookup |
+---------+  +---------+  +--------+
    ^             ^            ^
    |             |            |
    |             |            |
+----------------------------------+
|             MetaServer           |
+----------------------------------+
```

TODO
---
* [ ] 解决文件打开状态下，attribute 中 size 的对错，以发出 rpc 时 openfiles 中的 size 为准
* [ ] openfile size 开启 lru size
* [ ] 如果想达到原来但挂载的情况，就把内核缓存调到足够大
* [ ] 给 dir_cache 增加缓存时长？
* [ ] openfile cache 增加定时刷新一些属性到 MS，包括 length 和 mtime, ctime 这些
    * 记录初次打开的时间，如果这个有更新则直接刷新到 metaserver
* [ ] openfile 中的缓存淘汰时，要更新 dir cache 中的 DirEntry

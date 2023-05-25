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
+-------------------------------------------------------+
|                       Kernel Cache                    |  (L1 Cache)
+-------------------------------------------------------+
                            ^  (update length)
                            |
+--------------------------------------------------------+
|                       Open File Cache                  |  (L2 Cache)
|                   (only length and chunk)              |
+--------------------------------------------------------+
         ^                            ^
         | (reply entry/attr)         |  (reply entry/attr)
+-----------------+                   |
| Directory Cache |  (L3 Cache)       |
+-----------------+                   |
         ^                            |
         |                            |
+-----------------+ +------------------------------------+
|     readdir     | | lookup、getattr、setattr、create... |
+-----------------+ +------------------------------------+
         ^                            ^
         |                            |
+--------------------------------------------------------+
|                        MetaServer                      |
+--------------------------------------------------------+
```
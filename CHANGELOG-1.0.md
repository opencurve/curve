See [code changes](https://github.com/opencurve/curve/compare/v0.1.4...release1.0).

 ## The new features:
- Support external ip for ChunkServer to divide inter-cluster communication and communication with clients.
- The Curve_ops_tool supports printing space-info for each logical pool.
- Add snapshot clone tool.
- The curve-ansible adds deploy_monitor.yml.

## optimization
- Braft completely non-intrusive modifications.
- Replace HMacSha256 implementation.

## bug fix
- Fixed the bug that chunkserver_deploy.sh not able to delete the previous fstab record when deploy one.
- Fixed mds abort when exiting.
- Fixed curvefs_tool (curve) missing support for password.
- Fixed the bug that data from Etcd would not placed in LRU cache.
- Fixed ChunkServer not aborting when read returns an internal error.

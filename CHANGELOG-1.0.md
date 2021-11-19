See [code changes](https://github.com/opencurve/curve/compare/v0.1.4...release1.0).
# CHANGELOG of v1.0.0
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


<hr/>

# CHANGELOG of v1.0.3-rc0

## New Feature

- [change max file length from 4TB to 20TB](https://github.com/opencurve/curve/pull/321)


<hr/>

# CHANGELOG of v1.0.4

## New Feature

- [support nbd auto map at boot](https://github.com/opencurve/curve/pull/347)


<hr/>

# CHANGELOG of v1.0.5

## Optimization

- [Change auto map service to systemd standard](https://github.com/opencurve/curve/pull/695)

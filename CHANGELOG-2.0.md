# CHANGELOG of v2.0

The content of Curve v2.0 includes CurveBS v1.3, CurveFS v0.2.0 and some other content listed below.
Previous change logs can be found at [CHANGELOG-1.3](https://github.com/opencurve/curve/blob/master/CHANGELOG-1.3.md)

## new features

- [CurveFS: support replace a server which doesn't work anymore.](https://github.com/opencurve/curve/pull/954)
- [CurveFS: support internal and external services.](https://github.com/opencurve/curve/issues/973)


## optimization

- [CurveFS client: update inode asynchronously.](https://github.com/opencurve/curve/pull/1020)
- [Turn off batch delete object in the config file.](https://github.com/opencurve/curve/pull/997)
- [Remove create filesystem at mount process and use curvefs-tool instead.](https://github.com/opencurve/curve/pull/899)
- [Optimizing delete filesystem confirmation of curvefs-tool.](https://github.com/opencurve/curve/issues/843)
- [Optimizing leaky bucket algorithm of quality of service.](https://github.com/opencurve/curve/pull/1045)


## bug fix

- [Fix insert inode fail when load deleting partition.](https://github.com/opencurve/curve/pull/997)
- [Fix read fail when write cache of diskcache is enabled, because metadata maybe newer than data in S3.](https://github.com/opencurve/curve/pull/1006)
- [Fix CurveFS mds heartbeat doesn't delete copyset creating.](https://github.com/opencurve/curve/pull/1011)
- [Fix CurveFS create copyset when exist already.](https://github.com/opencurve/curve/pull/1002)

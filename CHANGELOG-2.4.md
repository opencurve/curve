# CHANGELOG of v2.4

Previous change logs can be found at [CHANGELOG-2.3](https://github.com/opencurve/curve/blob/master/CHANGELOG-2.3.md)

## Notable Changes

- [Update aws-sdk-cpp version to 1.9](https://github.com/opencurve/curve/pull/1780)

- [add feature of warmup](https://github.com/opencurve/curve/pull/1793)


## new features

- [CurveFS: implement setxattr interface](https://github.com/opencurve/curve/pull/1935)

- [update aws-sdk-cpp: add ip and custom port support](https://github.com/opencurve/curve/pull/1795)

- [CurveFS client: add s3.useVirtualAddressing config, default value: false](https://github.com/opencurve/curve/pull/1253)

- [Update aws-sdk-cpp version to 1.9](https://github.com/opencurve/curve/pull/1780)

- [add feature of warmup](https://github.com/opencurve/curve/pull/1793)

## optimization

- [CurveFS: update attr and extent in single rpc](https://github.com/opencurve/curve/pull/1784)

- [copyset schedule select copyset random](https://github.com/opencurve/curve/pull/1811)

- [CurveFS: only update dirty inode metadata](https://github.com/opencurve/curve/pull/1853)


## bug fix

- [CurveFS:  fix RefreshInode do not refresh when inode exist](https://github.com/opencurve/curve/pull/1800)

- [fix misuse Locked & Unlocked calling contract](https://github.com/opencurve/curve/pull/1279)

- [make callback of s3async request async to avoid deadlock](https://github.com/opencurve/curve/pull/1854)

- [CurveFS :fix update nlink error ](https://github.com/opencurve/curve/pull/1901)

- [CurveFS client: fix umount bug](https://github.com/opencurve/curve/pull/1923)

- [CurveFS client:  fix refresh inode will overwrite data in cache when enabel cto.](https://github.com/opencurve/curve/pull/1993)



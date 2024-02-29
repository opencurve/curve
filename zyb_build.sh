bazel build //src/chunkserver:chunkserver --copt -DHAVE_ZLIB=1 --copt -O2 -s --define=with_glog=true \
#bazel build //src/tools:curve_format --copt -DHAVE_ZLIB=1 --copt -O2 -s --define=with_glog=true \
#bazel build //src/client:all  --copt -DHAVE_ZLIB=1 --copt -O2 -s --define=with_glog=true \
#bazel build //:curvebs-sdk  --copt -DHAVE_ZLIB=1 --copt -O2 -s --define=with_glog=true \
	--define=libunwind=true --copt -DGFLAGS_NS=google --copt \
	-Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=1.2.6-zyb \
	--linkopt -L/usr/local/lib 
exit
--copt -faligned-new


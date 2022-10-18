
.PHONY: download build clean

build: clean download
	@cd libmemcached-1.1.2 && mkdir build-libmemcached && cd build-libmemcached && cmake .. && make libmemcached
	@cp libmemcached-1.1.2/build-libmemcached/include/libmemcached-1.0/configure.h libmemcached-1.1.2/include/libmemcached-1.0

download: clean
	@wget https://curve-build.nos-eastchina1.126.net/memcache/libmemcached-1.1.2.tar.gz && tar -zxf libmemcached-1.1.2.tar.gz

clean:
	@rm -fr libmemcached*
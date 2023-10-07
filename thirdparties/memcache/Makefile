
.PHONY: download build clean

build: clean download
	@cd libmemcached-1.1.4 && mkdir build && cd build && cmake .. -DBUILD_SHARED_LIBS=OFF && make libmemcached -j$$(nproc)
	@cp libmemcached-1.1.4/build/include/libmemcached-1.0/configure.h libmemcached-1.1.4/include/libmemcached-1.0

download: clean
	@wget https://github.com/awesomized/libmemcached/archive/refs/tags/1.1.4.tar.gz -O libmemcached-1.1.4.tar.gz && tar -zxf libmemcached-1.1.4.tar.gz

clean:
	@rm -fr libmemcached*

.PHONY: download build install clean

prefix?= "$(PWD)"

download:
	@git clone -b 6.28.fb https://gitee.com/mirrors/rocksdb.git

build: download
	@cd rocksdb && USE_RTTI=1 make static_lib

install:
	@cd rocksdb && make install PREFIX=$(prefix)

clean:
	@rm -rf rocksdb include lib

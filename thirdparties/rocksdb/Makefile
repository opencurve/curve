.PHONY: download build install clean

prefix?= "$(PWD)"

download:
	@wget https://curve-build.nos-eastchina1.126.net/rocksdb-6.28.fb.tar.gz && tar -zxvf rocksdb-6.28.fb.tar.gz
	#@git clone -b 6.28.fb https://gitee.com/mirrors/rocksdb.git

build: clean download
	@cd rocksdb && USE_RTTI=1 PORTABLE=1 make static_lib

install:
	@cd rocksdb && make install PREFIX=$(prefix)

clean:
	@rm -rf rocksdb* include lib

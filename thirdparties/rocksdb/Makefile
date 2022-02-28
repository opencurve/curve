.PHONY: download build install clean

prefix?= "$(PWD)"
from_source?= 0

download:
ifeq ($(from_source), 1)
	@git clone -b 6.28.fb https://gitee.com/mirrors/rocksdb.git
else
	@wget https://curve-build.nos-eastchina1.126.net/rocksdb-6.28.fb.tar.gz && tar -zxvf rocksdb-6.28.fb.tar.gz
endif

build: clean download
	@cd rocksdb && USE_RTTI=1 PORTABLE=1 make static_lib -j$(nproc)

install:
	@cd rocksdb && make install PREFIX=$(prefix)

clean:
	@rm -rf rocksdb* include lib

.PHONY: download build clean

prefix?= "$(PWD)"
tarball?= 1

build: clean download
	@cd aws-sdk-cpp && mkdir build && cd build && cmake .. -DBUILD_ONLY="s3-crt" -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=OFF && make -j$(nproc)

download: clean
ifeq ($(tarball), 1)
	@wget https://curve-build.nos-eastchina1.126.net/aws-sdk-cpp-e2cc5dd.tar.gz && tar -zxf aws-sdk-cpp-e2cc5dd.tar.gz
else
	@git submodule update --init --recursive --recommend-shallow --progress -- aws-sdk-cpp
endif


clean:
	@rm -rf aws-sdk-cpp aws-sdk-cpp-*.tar.gz

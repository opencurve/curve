#!/bin/bash
set -x
set -e

rm -rf build coverage lcov

git clone https://github.com/linux-test-project/lcov.git
cd lcov && make install && cd ..

update-alternatives --install /usr/bin/gcc gcc /usr/lib/gcc-4.9-backport/bin/gcc 90\
 --slave /usr/bin/g++ g++ /usr/lib/gcc-4.9-backport/bin/g++ \
 --slave /usr/bin/gcov gcov /usr/lib/gcc-4.9-backport/bin/gcov

./do_cmake
cd build
ctest -V

if [[ $? -ne 0 ]]; then
  exit 1
fi

cd ..
mkdir coverage
gcda=$(find $(pwd)/build/src -name *.gcda)
for i in ${gcda}; do
  j="$(echo $i | rev | cut -d . -f 2- | rev).gcno"
  cp --parents $i coverage
  cp --parents $j coverage
done

lcov --directory coverage$(pwd)/build --capture --output-file cov.info --rc lcov_branch_coverage=1
lcov --remove cov.info '/usr/*' '*3rdparty*' '*/build/*' -o result.info --rc lcov_branch_coverage=1
genhtml result.info --branch-coverage --ignore-errors source -o result
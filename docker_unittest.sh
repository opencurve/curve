#!/bin/bash
set -x
set -e

rm -rf build coverage

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

lcov --directory coverage$(pwd)/build --capture --output-file post.info
lcov --directory coverage$(pwd)/build --capture --initial --output-file raw.info
lcov -a post.info -a raw.info -o cov.info --rc lcov_branch_coverage=1
lcov --remove cov.info '/usr/*' '*3rdparty*' '*/build/*' -o result.info
genhtml result.info --branch-coverage --ignore-errors source -o result
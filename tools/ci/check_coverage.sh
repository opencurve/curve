#!/usr/bin/env bash

### Check coverage from coverage reports

set -e

if [ ! -d coverage ]; then
    echo "coverage report not found"
    exit -1
fi

check_repo_line_coverage() {
    expected_coverage=$1
    actual_coverage=$(cat coverage/index.html | grep -A5 "Lines" | grep % | awk -F'>' '{ print $2 }' | awk '{ print $1 }')

    if (($(echo "$actual_coverage >= $expected_coverage" | bc -l))); then
        echo "repo line coverage ratio: $actual_coverage"
    else
        echo "repo line coverage isn't ok, actual coverage ratio: $actual_coverage, expected coverage ratio: $expected_coverage"
        exit -1
    fi
}

check_repo_branch_coverage() {
    expected_coverage=$1
    actual_coverage=$(cat coverage/index.html | grep -A5 "Branches" | grep % | awk -F'>' '{ print $2 }' | awk '{ print $1 }' | grep -v tr)

    if (($(echo "$actual_coverage >= $expected_coverage" | bc -l))); then
        echo "repo branch coverage ratio: $actual_coverage"
    else
        echo "repo branch coverage isn'tvim ok, actual coverage ratio: $actual_coverage, expected coverage ratio: $expected_coverage"
        exit -1
    fi
}

check_module_branch_coverage() {
    base_dir=$1
    expected_coverage=$2

    find coverage/$base_dir -name index.html | xargs cat | grep -A5 "Branches" | grep % | awk -F'>' '{ print $2 }' | awk '{ print $1 }' | grep -v tr >dummy.all
    if [ -s dummy.all ]; then
        actual_coverage=$(cat dummy.all | awk '{ sum+= $1 } END { print sum/NR }')
        rm dummy.all || true
    else
        actual_coverage=0
    fi

    if (($(echo "$actual_coverage >= $expected_coverage" | bc -l))); then
        echo "$base_dir branch coverage ratio: ${actual_coverage}"
    else
        echo "$base_dir branch coverage is not ok, actual branch coverage ratio: ${actual_coverage}, expected branch coverage ratio: ${expected_coverage}"
        exit -1
    fi
}
if [ $1 == "curvebs" ];then

check_repo_branch_coverage 59
check_repo_line_coverage 76

## two arguments are module and expected branch coverage ratio
check_module_branch_coverage "src/mds" 70
check_module_branch_coverage "src/client" 78
check_module_branch_coverage "src/chunkserver" 65
check_module_branch_coverage "src/snapshotcloneserver" 65
check_module_branch_coverage "src/tools" 65
check_module_branch_coverage "src/common" 65
check_module_branch_coverage "src/fs" 65
check_module_branch_coverage "src/idgenerator" 79
check_module_branch_coverage "src/kvstorageclient" 70
check_module_branch_coverage "src/leader_election" 100
check_module_branch_coverage "nebd" 75

elif [ $1 == "curvefs" ];then

check_module_branch_coverage "mds" 59
check_module_branch_coverage "client" 59
check_module_branch_coverage "metaserver" 65
check_module_branch_coverage "common" 16
check_module_branch_coverage "tools" 0
fi

echo "Checking repo coverage succeeded!"

exit 0

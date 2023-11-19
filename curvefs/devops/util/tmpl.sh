#!/usr/bin/env bash

# Usage:
#   tmpl.sh DSV SOURCE DESTINATION
# Example:
#   tmpl.sh = /usr/local/metaserver.conf /tmp/metaserver.conf

g_dsv=$1
g_src=$2
g_dst=$3
g_regex="^([^$g_dsv]+$g_dsv[[:space:]]*)(.+)__ANSIBLE_TEMPLATE__[[:space:]]+(.+)[[:space:]]+__ANSIBLE_TEMPLATE__(.*)$"
while IFS= read -r line; do
    if [[ ! $line =~ $g_regex ]]; then
        echo $line
    else
        echo ${BASH_REMATCH[1]}${BASH_REMATCH[3]}
    fi
done < $g_src > $g_dst

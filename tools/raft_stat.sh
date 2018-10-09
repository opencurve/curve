#!/bin/sh

usage() {
	echo "Usage: raft_stat.sh <IP> <PORT>"
	exit 1
}

[ $# -ne 2 ] && usage

IP=$1
PORT=$2

curl -L http://${IP}:${PORT}/raft_stat 2>/dev/null

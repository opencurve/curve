#
#     Copyright (c) 2020 NetEase Inc.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#

#!/bin/sh

#sh update_dashboard.sh
#echo "update dashboards success!"

WORKDIR=/etc/curve/monitor

if [ ! -d $WORKDIR ]; then
  echo "${WORKDIR} not exists"
  exit 1
fi

cd $WORKDIR
chmod -R 777 prometheus
chmod -R 777 grafana

start() {
    echo "==========start==========="

    echo "" > monitor.log

    stdbuf -oL python target_json.py >> monitor.log 2>&1 &
    echo "start prometheus targets service success!"

    docker-compose up >> monitor.log 2>&1 &
    echo "start metric system success!"
}

stop() {
    echo "===========stop============"

    docker-compose down

    ID=`(ps -ef | grep "target_json.py"| grep -v "grep") | awk '{print $2}'`
    for id in $ID
    do
        kill -9 $id
	echo "killed $id"
    done
}

restart() {
    stop
    echo "sleeping........."
    sleep 3
    start
}

case "$1" in
    'start')
        start
        ;;
    'stop')
        stop
        ;;
    'status')
        status
        ;;
    'restart')
        restart
        ;;
    *)
    echo "usage: $0 {start|stop|restart}"
    exit 1
        ;;
    esac

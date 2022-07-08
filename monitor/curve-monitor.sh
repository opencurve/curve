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

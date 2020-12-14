#!/bin/bash


# default confpath
confPath=/etc/nbd/k8s_curve.conf


# usage
function usage() {
    echo "Usage: ./umount_curve_clouddisk.sh"
    echo "  -c/--confPath: set the confPath (default /etc/nbd/k8s_curve.conf)"
    echo "  --filename: name of the curve file"
    echo "  --user: owner of the curve file"
    echo "  -h/--help: get the script usage"
    echo "Note: the explicit OPTIONS will override the confPath OPTIONS!!!"
    echo "Examples:"
    echo "  ./umount_curve_clouddisk.sh //use the default configuration"
    echo "  ./umount_curve_clouddisk.sh --confPath yourConfPath //use the new confPath"
    echo "  ./umount_curve_clouddisk.sh --filename curvefile --user k8s //use specify OPTIONS"
}

function umount() {
    sudo umount $nbddevice
    hasmount=$(df -T | grep ${nbddevice})
    if [ "$hasmount" ]
    then
        echo "ERROR: umount nbddevice failed!"
        exit
    else
        echo "umount nbddevice success"
    fi
}

function unmap() {
    sudo curve-nbd unmap $nbddevice
    cnt=$(ps -ef | grep -E "curve-nbd map cbd:pool/${filepath}" | grep -v grep | wc -l)
    while [ ! "$cnt" -eq 0 ]
    do
        sleep 0.5
        cnt=$(ps -ef | grep -E "curve-nbd map cbd:pool/${filepath}" | grep -v grep | wc -l)
    done

    nbddevice=$(sudo curve-nbd list-mapped | grep ${filepath} | cut -d " " -f 3)
    if [ "$nbddevice" ]
    then
        echo "ERROR: unmap nbddevice failed!"
        exit
    else
        echo "unmap nbddevice success"
    fi
}

function delvolume() {
    read -r -p "Delete curvevolume filename=${filepath}, user=${user}. Are You Sure? [Y/n] " input
    case $input in
        [yY][eE][sS]|[yY])
            sudo curve delete --filename $filepath --user $user
            curvevolume=$(sudo curve list --user ${user} --dirname /${user} | grep -E "^${filename}$")
            vexist=$(sudo curve list --user root --dirname / --password root_password | grep -E "^${filename}$")
            if [ "$curvevolume" -o "$vexist" ]
            then
                echo "ERROR: delete curve volume failed!"
                exit
            else
                echo "delete curve volume success"
            fi
            ;;
        [nN][oO]|[nN])
            exit 1
            ;;
        *)
        echo "Invalid input..."
        exit 1
        ;;
    esac
}

function run() {
    # check required OPTIONS
    if [ ! "$filename" -o ! "$user" ]
    then
        echo "ERROR: the required OPTIONS missed!"
        usage
        exit
    fi

    curvevolume=$(sudo curve list --user ${user} --dirname /${user} | grep -E "^${filename}$")
    vexist=$(sudo curve list --user root --dirname / --password root_password | grep -E "^${filename}$")
    if [ ! "$curvevolume" -a ! "$vexist" ]
    then
        echo "clean success"
        exit
    elif [ "$curvevolume" -a "$vexist" ]
    then
        echo "ERROR: volume: ${filename} exist both in root dir and /${user} dir"
        exit
    else
        if [ "$vexist" ]
        then
            filepath=/${filename}
        else
            filepath=/${user}/${filename}
        fi

        nbddevice=$(sudo curve-nbd list-mapped | grep ${filepath} | cut -d " " -f 3)
        if [ ! "$nbddevice" ]
        then
            delvolume
        else
            hasmount=$(df -T | grep ${nbddevice})
            if [ ! "$hasmount" ]
            then
                unmap
                delvolume
                echo "clean success"
            else
                umount
                unmap
                delvolume
                echo "clean success"
            fi
        fi
    fi
}

# get required OPTIONS from confPath
function getOptions() {
    filename=`cat ${confPath} | grep -v '#' | grep filename= | awk -F "=" '{print $2}'`
    user=`cat ${confPath} | grep -v '#' | grep user= | awk -F "=" '{print $2}'`
}

count=1
for i in "$@"
do
    if [ "$i" == "-c" -o "$i" == "--confPath" ]
    then
        let count+=1
        eval confPath=$(echo \$$count)
        break
    else
        let count+=1
    fi
done

if [ ! -f ${confPath} ]
then
    echo "ERROR: not found configuration file, confPath is ${confPath}!"
    exit
else
    getOptions
    while [[ $# -gt 0 ]]
    do
        key=$1

        case $key in
        -c|--confPath)
            confPath="$2"
            shift 2 # pass key & value
            ;;
        --filename)
            filename="$2"
            shift 2
            ;;
        --user)
            user="$2"
            shift 2
            ;;
        *)
            usage
            exit
            ;;
        esac
    done
    run
fi
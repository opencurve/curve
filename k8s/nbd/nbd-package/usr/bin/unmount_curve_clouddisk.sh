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

function run() {
    # check required OPTIONS
    if [ ! "$filename" -o ! "$user" ]
    then
        echo "ERROR: the required OPTIONS missed!"
        usage
        exit
    fi

    curvevolume=$(sudo curve_ops_tool list --fileName=/ | grep ${filename})
    nbddevice=$(sudo curve-nbd list-mapped | grep ${filename} | cut -d " " -f 3)
    hasmount=$(df -T | grep ${nbddevice})

    if [ "$hasmount" ]
    then
        sudo umount $nbddevice
    fi

    if [ "$nbddevice" ]
    then
        sudo curve-nbd unmap $nbddevice
    fi

    if [ "$curvevolume" ]
    then
        read -r -p "Delete curvevolume filename=${filename}, user=${user}. Are You Sure? [Y/n] " input
        case $input in
            [yY][eE][sS]|[yY])
                sudo curve delete --filename /$filename --user $user
                echo "Delete curvevolume done"
                ;;
            [nN][oO]|[nN])
                exit 1
                ;;
            *)
            echo "Invalid input..."
            exit 1
            ;;
        esac
    fi
}

# get required OPTIONS from confPath
function getOptions() {
    filename=`cat ${confPath} | grep -v '#' | grep filename= | awk -F "=" '{print $2}'`
    user=`cat ${confPath} | grep -v '#' | grep user= | awk -F "=" '{print $2}'`
    mountpoint=`cat ${confPath} | grep -v '#' | grep mountpoint= | awk -F "=" '{print $2}'`
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
        --mountpoint)
            mountpoint="$2"
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
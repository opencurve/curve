#!/bin/bash


# default confpath
confPath=/etc/nbd/k8s_curve.conf


# usage
function usage() {
    echo "Usage: ./mount_curve_clouddisk.sh [OPTIONS...]"
    echo "  -c/--confPath: set the confPath (default /etc/nbd/k8s_curve.conf)"
    echo "  --filename: name of the curve file"
    echo "  --filelength: length of the curve file (unit GB)"
    echo "  --user: owner of the curve file"
    echo "  -h/--help: get the script usage"
    echo "Note: the explicit OPTIONS will override the confPath OPTIONS!!!"
    echo "Examples:"
    echo "  ./mount_curve_clouddisk.sh //use the default configuration"
    echo "  ./mount_curve_clouddisk.sh --confPath yourConfPath //use the new confPath"
    echo "  ./mount_curve_clouddisk.sh --filename curvefile --filelength 1024 --user k8s //use specify OPTIONS"
}

function run() {
    # check required OPTIONS
    if [ ! "$filename" -o ! "$filelength" -o ! "$user" ]
    then
        echo "ERROR: the required OPTIONS missed!"
        usage
        exit
    fi

    # create CURVE volume
    echo "1. create CURVE volume"
    # check volume exist in root dir
    vexist=$(sudo curve list --user root --dirname / --password root_password | grep -E "^${filename}$")
    if [ "$vexist" ]
    then
        filepath=/${filename}
        echo "CURVE volume /${filename} exists"
    else
        # check user dir exist in root dir
        userdir=$(sudo curve list --user root --dirname / --password root_password | grep -E "^${user}$")
        if [ ! "$userdir" ]
        then
            sudo curve mkdir --user ${user} --dirname /${user}
        else
            echo "CURVE user dir /${user} exists"
        fi
        # check the user dir create success or not
        userdir=$(sudo curve list --user root --dirname / --password root_password | grep -E "^${user}$")
        if [ ! "$userdir" ]
        then
            echo "ERROR: create CURVE user dir error!"
            exit
        else
            # check volume exist in user dir
            volume=$(sudo curve list --user ${user} --dirname /${user} | grep -E "^${filename}$")
            if [ ! "$volume" ]
            then
                echo "CURVE volume info: filename: /${user}/${filename}, filelength: ${filelength}, user: ${user}"
                sudo curve create --filename /${user}/${filename} --length ${filelength} --user ${user}
            else
                echo "CURVE volume /${user}/${filename} exists"
            fi
            filepath=/${user}/${filename}
        fi
    fi

    # map curve volume to nbd device
    echo "2. map CURVE volume to nds device"
    volume=$(sudo curve list --user ${user} --dirname /${user} | grep -E "^${filename}$")
    if [ ! "$vexist" -a ! "$volume" ]
    then
        echo "ERROR: create CURVE volume error!"
        exit
    else
        nbddevice=$(sudo curve-nbd list-mapped | grep ${filepath})
        if [ ! "$nbddevice" ]
        then
            echo "curve-nbd map cbd:pool/${filepath}_${user}_"
            sudo curve-nbd map cbd:pool/${filepath}_${user}_
        else
            echo "CURVE volume ${filepath} has mapped"
        fi
    fi

    # format the nbd device to ext4 filesystem
    echo "3. format the nbd device to ext4 filesystem"
    nbddevice=$(sudo curve-nbd list-mapped | grep ${filepath} | cut -d " " -f 3)
    if [ ! "$nbddevice" ]
    then
        echo "ERROR: map curve volume to nbd device error!"
        exit
    else
        fstype=$(sudo blkid -s TYPE ${nbddevice} | awk -F '"' '{print $2}')
        if [ "$fstype" != "ext4" ]
        then
            echo "format ${nbddevice} to ext4"
            sudo mkfs.ext4 ${nbddevice}
        else
            echo "nbd device ${nbddevice} has been formatted"
        fi
        sed -i "/^What=*/cWhat=$nbddevice" /etc/systemd/system/data.mount
    fi
}

# get required OPTIONS from confPath
function getOptions() {
    filename=`cat ${confPath} | grep -v '#' | grep filename= | awk -F "=" '{print $2}'`
    filelength=`cat ${confPath} | grep -v '#' | grep filelength= | awk -F "=" '{print $2}'`
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
        --filelength)
            filelength="$2"
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
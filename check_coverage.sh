#!/bin/bash 

line_cover_base=75
mds_branch_base=70
snapshot_branch_base=70
other_branch_base=65
line_cover_all=`cat coverage/index.html | grep -A 5  "Lines"  | grep % | awk -F "%" '{print $1}' | awk -F '>' '{print $2}' | awk -F '.' '{print $1}'`

if(("$line_cover_all" < "$line_cover_base"))
then
    echo "line cover not ok!.";
    echo $line_cover_all;
    exit -1
else
    echo "line cover ok!.";
    echo $line_cover_all;    
fi

for i in `find coverage -type d | grep mds`;do for j in $i;do find $j -name index.html | xargs cat | grep -A 5 "Branches" | grep % | awk -F '>' '{print $2}' | awk '{print $1}' | awk -F '.' '{print $1}' | grep -v tr;done;done > mds.all
if [ -s mds.all ]; then    
    mds_branch=`cat mds.all | awk '{sum+=$1} END {print sum/NR}' | awk -F '.' '{print $1}'`
else 
    mds_branch=0
fi
if(("$mds_branch" < "$mds_branch_base"))
then
    echo "mds_branch cover not ok!.";
    echo $mds_branch;
    exit -1
else
    echo "mds_branch cover ok!.";
    echo $mds_branch;    
fi

for i in `find coverage -type d | grep snapshot`;do for j in $i;do find $j -name index.html | xargs cat | grep -A 5 "Branches" | grep % | awk -F '>' '{print $2}' | awk '{print $1}' | awk -F '.' '{print $1}' | grep -v tr;done;done > snapshot.all
if [ -s snapshot.all ]; then
    snapshot_branch=`cat snapshot.all | awk '{sum+=$1} END {print sum/NR}' | awk -F '.' '{print $1}'`
else
    snapshot_branch=0
fi
if(("$snapshot_branch" < "$snapshot_branch_base"))
then
    echo "snapshot branch cover not ok!.";
    echo $snapshot_branch;
    exit -1
else
    echo "snapshot branch cover ok!.";
    echo $snapshot_branch;
fi

for i in `find coverage -type d | grep tools`;do for j in $i;do find $j -name index.html | xargs cat | grep -A 5 "Branches" | grep % | awk -F '>' '{print $2}' | awk '{print $1}' | awk -F '.' '{print $1}' | grep -v tr;done;done > tools.all
if [ -s tools.all ]; then
    tools_branch=`cat tools.all | awk '{sum+=$1} END {print sum/NR}' | awk -F '.' '{print $1}'`
else 
    tools_branch=0
fi

if(("$tools_branch" < "$other_branch_base"))
then
    echo "tools_branch cover not ok!.";
    echo $tools_branch;
    #exit -1
else
    echo "tools_branch cover ok!.";
    echo $tools_branch;    
fi

for i in `find coverage -type d | grep common`;do for j in $i;do find $j -name index.html | xargs cat | grep -A 5 "Branches" | grep % | awk -F '>' '{print $2}' | awk '{print $1}' | awk -F '.' '{print $1}' | grep -v tr;done;done > common.all
if [ -s common.all ]; then
    common_branch=`cat common.all | awk '{sum+=$1} END {print sum/NR}' | awk -F '.' '{print $1}'`
else 
    common_branch=0
fi	
if(("$common_branch" < "$other_branch_base"))
then
    echo "common_branch cover not ok!.";
    echo $common_branch;
    exit -1
else
    echo "common_branch cover ok!.";
    echo $common_branch;    
fi


for i in `find coverage -type d | grep chunkserver`;do for j in $i;do find $j -name index.html | xargs cat | grep -A 5 "Branches" | grep % | awk -F '>' '{print $2}' | awk '{print $1}' | awk -F '.' '{print $1}' | grep -v tr;done;done > chunkserver.all
if [ -s chunkserver.all ]; then
    chunkserver_branch=`cat chunkserver.all | awk '{sum+=$1} END {print sum/NR}' | awk -F '.' '{print $1}'`
else 
    chunkserver_branch=0
fi
if(("$chunkserver_branch" < "$other_branch_base"))
then
    echo "chunkserver_branch cover not ok!.";
    echo $chunkserver_branch;
    exit -1
else
    echo "chunkserver_branch cover ok!.";
    echo $chunkserver_branch;    
fi


for i in `find coverage -type d | grep client`;do for j in $i;do find $j -name index.html | xargs cat | grep -A 5 "Branches" | grep % | awk -F '>' '{print $2}' | awk '{print $1}' | awk -F '.' '{print $1}' | grep -v tr;done;done > client.all
if [ -s chunkserver.all ]; then
    client_branch=`cat client.all | awk '{sum+=$1} END {print sum/NR}' | awk -F '.' '{print $1}'`
else 
    client_branch=0
fi
if(("$client_branch" < "$other_branch_base"))
then
    echo "client_branch cover not ok!.";
    echo $client_branch;
    exit -1
else
    echo "client_branch cover ok!.";
    echo $client_branch;    
fi

for i in `find coverage -type d | grep fs`;do for j in $i;do find $j -name index.html | xargs cat | grep -A 5 "Branches" | grep % | awk -F '>' '{print $2}' | awk '{print $1}' | awk -F '.' '{print $1}' | grep -v tr;done;done > sfs.all
if [ -s sfs.all ]; then
    sfs_branch=`cat sfs.all | awk '{sum+=$1} END {print sum/NR}' | awk -F '.' '{print $1}'`
else 
    sfs_branch=0
fi
if(("$sfs_branch" < "$other_branch_base"))
then
    echo "sfs_branch cover not ok!.";
    echo $sfs_branch;
    #exit -1
else
    echo "sfs_branch cover ok!.";
    echo $sfs_branch;    
fi

for i in `find coverage -type d | grep repo`;do for j in $i;do find $j -name index.html | xargs cat | grep -A 5 "Branches" | grep % | awk -F '>' '{print $2}' | awk '{print $1}' | awk -F '.' '{print $1}' | grep -v tr;done;done > repo.all
if [ -s repo.all ]; then
    repo_branch=`cat repo.all | awk '{sum+=$1} END {print sum/NR}' | awk -F '.' '{print $1}'`
else
    repo_branch=0
fi
if(("$repo_branch" < "$other_branch_base"))
then
    echo "repo_branch cover not ok!.";
    echo $repo_branch;
    #exit -1
else
    echo "repo_branch cover ok!.";
    echo $repo_branch;
fi

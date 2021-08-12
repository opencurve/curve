set -ex
#!/bin/bash
conf_url="/var/lib/jenkins/workspace/ansibe-conf"

bash replace-curve-repo.sh
bash mk-tar.sh debug
ls *.tar.gz | xargs -n1 tar xzvf
\cp ${conf_url}/server.ini curve/curve-ansible/
\cp ${conf_url}/client.ini curve/curve-ansible/
\cp ${conf_url}/group_vars/mds.yml curve/curve-ansible/group_vars/
cd curve/curve-ansible
ansible-playbook -i client.ini clean_nebd.yml 
ansible-playbook -i server.ini clean_curve.yml
ansible-playbook -i server.ini clean_curve.yml --tags snapshotclone

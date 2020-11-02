set -ex
#!/bin/bash
conf_url="/var/lib/jenkins/workspace/ansibe-conf"

bash replace-curve-repo.sh
bash mk-tar.sh
ls *.tar.gz | xargs -n1 tar xzvf
\cp ${conf_url}/server.ini curve/curve-ansible/
\cp ${conf_url}/client.ini %s/curve/curve-ansible/
\cp %s/group_vars/mds.yml %s/curve/curve-ansible/group_vars/
cd curve/curve-ansible
ansible-playbook -i server.ini deploy_curve.yml --skip-tags prepare_software_env
ansible-playbook -i server.ini deploy_curve.yml --tags snapshotclone
ansible-playbook -i client.ini deploy_curve_sdk.yml --skip-tags prepare_software_env
ansible-playbook -i client.ini deploy_nebd.yml --skip-tags prepare_software_env
ansible-playbook -i client.ini deploy_nbd.yml --skip-tags prepare_software_env,check_kernal_version

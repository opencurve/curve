set -ex
#!/bin/bash
conf_url="/var/lib/jenkins/workspace/ansibe-conf"

bash replace-curve-repo.sh
bash mk-tar.sh
ls *.tar.gz | xargs -n1 tar xzvf
\cp ${conf_url}/server.ini curve/curve-ansible/
\cp ${conf_url}/client.ini curve/curve-ansible/
\cp ${conf_url}/group_vars/mds.yml curve/curve-ansible/group_vars/
cd curve/curve-ansible
ansible-playbook -i client.ini clean_nebd.yml 
ansible-playbook -i server.ini clean_curve.yml
ansible-playbook -i server.ini clean_curve.yml --tags snapshotclone
ansible-playbook -i server.ini deploy_curve.yml --skip-tags prepare_software_env
ansible-playbook -i server.ini deploy_curve.yml --tags snapshotclone
ansible-playbook -i client.ini deploy_curve_sdk.yml --skip-tags prepare_software_env
ansible-playbook -i client.ini deploy_nebd.yml --skip-tags prepare_software_env
ansible-playbook -i client.ini deploy_nbd.yml --skip-tags prepare_software_env,check_kernel_version

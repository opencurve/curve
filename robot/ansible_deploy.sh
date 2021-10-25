set -ex
#!/bin/bash
cd curve/curve-ansible
ansible-playbook -i server.ini deploy_curve.yml --skip-tags prepare_software_env
ansible-playbook -i server.ini deploy_curve.yml --tags snapshotclone
ansible-playbook -i client.ini deploy_curve_sdk.yml --skip-tags prepare_software_env
ansible-playbook -i client.ini deploy_nebd.yml --skip-tags prepare_software_env
ansible-playbook -i client.ini deploy_nbd.yml --skip-tags prepare_software_env,check_kernel_version

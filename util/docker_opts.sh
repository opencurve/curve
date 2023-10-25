# This file must be used with "source docker_opts.sh" from bash
# you cannot run it directly.

g_docker_opts=(
    "-v ${HOME}:${HOME}"
    "--user $(id -u ${USER}):$(id -g ${USER})"
    "-v /etc/passwd:/etc/passwd:ro"
    "-v /etc/group:/etc/group:ro"
    "-v /etc/sudoers.d/:/etc/sudoers.d/"
    "-v /etc/sudoers:/etc/sudoers:ro"
    "-v /etc/shadow:/etc/shadow:ro"
    "-v /var/run/docker.sock:/var/run/docker.sock"
    "-v /root/.docker:/root/.docker"
    "--ulimit core=-1"
    "--privileged"
)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

import re
import json
#from __future__ import print_function

def re_match(url):
    mu = re.match(r'^(https?|local|deb)://([^/]+)?(.*?([^/]+))$', url)
    if mu:
        scheme, uri, filename = mu.group(1), mu.group(3), mu.group(4)
        mu = re.match(r'.+?([^-]+)\.tar\.gz$', uri)
        if mu:
            md5sum = mu.group(1)
            return scheme, uri, filename, md5sum

    raise AnsibleError("invalid tarball url : %s" % url)

# TODO(@Wine93): support debian package
#   local) local:///usr/local/metaserver-v0.0.1-123456.tar.gz
#   http) http://www.163.com/metaserver-v0.0.1-123456.tar.gz
def extract_url(url, field):
    scheme, uri, filename, md5sum = re_match(url)
    if field == 'scheme':
        return scheme
    elif field == 'uri':
        return uri
    elif field == 'filename':
        return filename
    elif field == 'md5sum':
        return md5sum

def join_peer(groups, hostvars, port_field):
    peers = []
    for peer in groups:
        peer_host = hostvars[peer]['ansible_ssh_host']
        peer_port = hostvars[peer][port_field]
        peers.append(("%s:%s") % (peer_host, peer_port))
    return ','.join(peers)

def join_etcd_peer(etcd_groups, hostvars):
    peers = []
    for peer in etcd_groups:
        peer_host = hostvars[peer]['ansible_ssh_host']
        peer_port = hostvars[peer]['etcd_listen_peer_port']
        peers.append(("%s=http://%s:%s") % (peer, peer_host, peer_port))
    return ','.join(peers)

def print_status(stdout):
    print ("\033[32m%s\033[0m: " % stdout[0])
    for i in range(1, len(stdout)):
        print (stdout[i])
    print ("")

def echo(str, title):
    print ("\033[32m%s\033[0m: \n%s" % (title, str))
    return str

# TODO(@Wine93): support more options
def smart_topology(metaserver_groups, hostvars, nzone, ncopyset):
    # pools
    pools = []
    pools.append({
        'name': 'pool1',
        'replicasnum': 3,
        'copysetnum': ncopyset,
        'zonenum': nzone
    })

    # metaservers
    idx = 1
    metaservers = []
    for peer in metaserver_groups:
        peer_host = hostvars[peer]['ansible_ssh_host']
        peer_port = int(hostvars[peer]['metaserver_listen_port'])
        metaserver = {
            'name': 'metaserver%d' % (idx),
            'internalip': peer_host,
            'internalport': peer_port,
            'externalip': peer_host,
            'externalport': peer_port,
        }
        idx += 1
        metaservers.append(metaserver)

    # servers
    idx = 1
    servers = []
    for metaserver in metaservers:
        server = {
            'name': 'server%d' % (idx),
            'internalip': metaserver['internalip'],
            'internalport': metaserver['internalport'],
            'externalip': metaserver['externalip'],
            'externalport': metaserver['externalport'],
            'zone': 'zone%d' % ((idx - 1) % nzone + 1),
            'pool': 'pool1',
        }
        idx += 1
        servers.append(server)

    topology = { 'servers': servers, 'pools': pools }
    return topology

class FilterModule(object):
    def filters(self):
        return {
            'extract_url': extract_url,
            'join_peer': join_peer,
            'join_etcd_peer': join_etcd_peer,
            'print_status': print_status,
            'echo': echo,
            'smart_topology': smart_topology,
        }

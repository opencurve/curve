#!/usr/bin/python
# -*- coding: utf-8 -*-

a = ['src/chunkserver/datastore/chunkfile_pool.cpp', 'src/fs/ext4_filesystem_impl.cpp', 'src/fs/wrap_posix.cpp']

a = [c.split('/', 1)[1] for c in a]
print a 



#!/usr/bin/env python
# -*- coding: utf8 -*-

from curvesnapshot_python import curvesnapshot
from config import config
from logger.logger import *


class CurveSnapshot:

    def __init__(self):
        rc = curvesnapshot.Init(config.client_conf)
        logger.info("CurveSnapshot init success.")
        if rc != 0:
            print ("init CurveSnapshot client fail! rc=%s" % rc)
            logger.error("init CurveSnapshot client fail! rc=%s" % rc)
            raise AssertionError

    def create_snapshot(self, file_path, user_name, password):
        user = curvesnapshot.CUserInfo_t()
        user.owner = user_name
        user.password = password
        seq = curvesnapshot.type_uInt64_t()
        rc = curvesnapshot.CreateSnapShot(file_path, user, seq)
        if rc != 0:
            print ("create_snapshot fail! rc=%s" % rc)
            logger.error("create_snapshot fail! rc=%s" % rc)
            return rc
        else:
            return seq

    def get_snapshot(self, file_path, user_name, password, seq):
        user = curvesnapshot.CUserInfo_t()
        user.owner = user_name
        user.password = password
        finfo = curvesnapshot.CFInfo_t()
        rc = curvesnapshot.GetSnapShot(file_path, user, seq, finfo)
        if rc != 0:
            print ("get_snapshot fail! rc=%s" % rc)
            logger.error("get_snapshot fail! rc=%s" % rc)
            return rc
        else:
            logger.info("get_sanpshot_info , file snapshot info.status = %s, owner = %s, filename = %s, "
                        "length = %s, chunksize = %s, seqnum = %s, segmentsize = %s" % (
                            finfo.filestatus, finfo.owner, finfo.filename, finfo.length.value, finfo.chunksize.value,
                            finfo.seqnum.value, finfo.segmentsize.value))
            return finfo

    def get_snapshot_SegmentInfo(self, file_path, user_name, password, seq, offset):
        user = curvesnapshot.CUserInfo_t()
        user.owner = user_name
        user.password = password
        segInfo = curvesnapshot.CSegmentInfo_t()
        rc = curvesnapshot.GetSnapshotSegmentInfo(file_path, user, seq, offset, segInfo)
        if rc != 0:
            logger.error("get_snapshot_SegmentInfo fail! rc=%s" % rc)
            return rc
        else:
            return segInfo

    def get_chunk_Info(self, chunkidinfo):
        info = curvesnapshot.CChunkInfoDetail_t()
        rc = curvesnapshot.GetChunkInfo(chunkidinfo, info)
        if rc != 0:
            print ("get_chunk_Info fail! rc=%s" % rc)
            logger.error("get_chunk_Info fail! rc=%s" % rc)
            return rc
        else:
            return info

    def read_chunk_snapshot(self, idinfo, seq, offset, len, buf):
        content = curvesnapshot.ReadChunkSnapshot(idinfo, seq, offset, len, buf)
        return content

    def delete_chunk_snapshot_or_correct_sn(self, idinfo, correctseq):
        rc = curvesnapshot.DeleteChunkSnapshotOrCorrectSn(idinfo, correctseq)
        if rc != 0:
            print ("delete_chunk_snapshot_or_correct_sn fail! rc=%s" % rc)
            logger.error("delete_chunk_snapshot_or_correct_sn fail! rc=%s" % rc)
        return rc

    def check_snapshot_status(self, file_path, user_name, password, seq):
        user = curvesnapshot.CUserInfo_t()
        user.owner = user_name
        user.password = password
        filestatus = curvesnapshot.type_uInt32_t()
        rc = curvesnapshot.CheckSnapShotStatus(file_path, user, seq, filestatus)
        if rc != 0:
            print ("check_snapshot_status fail! rc=%s" % rc)
            logger.error("check_snapshot_status fail! rc=%s" % rc)
            return rc
        else:
            return filestatus

    def delete_snapshot(self, file_path, user_name, password, seq):
        user = curvesnapshot.CUserInfo_t()
        user.owner = user_name
        user.password = password
        rc = curvesnapshot.DeleteSnapShot(file_path, user, seq)
        if rc != 0:
            print ("delete_snapshot fail! rc=%s" % rc)
            logger.error("delete_snapshot fail! rc=%s" % rc)
        return rc

    def create_clone_chunk(self, file_path, chunkinfo, seq, correctseq, chunksize):
        rc = curvesnapshot.CreateCloneChunk(file_path, chunkinfo, seq, correctseq, chunksize)
        if rc != 0:
            print ("create_clone_chunk fail! rc=%s" % rc)
            logger.error("create_clone_chunk fail! rc=%s" % rc)
        return rc

    def recover_chunk(self, chunkinfo, offset, len):
        rc = curvesnapshot.RecoverChunk(chunkinfo, offset, len)
        if rc != 0:
            print ("recover_chunk fail! rc=%s" % rc)
            logger.error("recover_chunk fail! rc=%s" % rc)
        return rc


def libcurve_uninit():
    rc = curvesnapshot.UnInit()
    if rc != None:
        print "CurveSnapshot uninit  fail! rc=%s" % rc
        logger.error("CurveSnapshot uninit file fail! rc=%s" % rc)
        return rc
        raise AssertionError
    else:
        return 0

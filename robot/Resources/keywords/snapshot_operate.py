#!/usr/bin/env python
# -*- coding: utf8 -*-

from config import config
from logger.logger import *
from swig import snapshot_client
from swig import swig_operate
from curvesnapshot_python import curvesnapshot


def create_curve_file_for_snapshot(file_name=config.snapshot_file_name, user_name=config.user_name, size=config.size,
                                   pass_word=config.pass_word):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_create(file_name, user_name, size, pass_word)
    if rc != 0:
        logger.info("create_curve_file_for_snapshot file fail. rc = %s" % rc)
        return rc
    else:
        return rc

def create_curve_file_for_snapshot_delete(file_name="/lc-delete", user_name=config.user_name, size=config.size,
                                   pass_word=config.pass_word):
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_create(file_name, user_name, size, pass_word)
    if rc != 0:
        logger.info("create_curve_file_for_snapshot file fail. rc = %s" % rc)
        return rc
    else:
        return rc

def delete_curve_file_for_shanpshot():
    curvefs = swig_operate.LibCurve()
    rc = curvefs.libcurve_delete(config.snapshot_file_name, config.user_name, config.pass_word)
    if rc != 0:
        logger.info("delete_curve_file_for_shanpshot file fail. rc = %s" % rc)
        return rc
    else:
        return rc


# open/write/close file
def write_curve_file_for_snapshot(file_name=config.snapshot_file_name, user_name=config.user_name,
                                  pass_word=config.pass_word,
                                  buf=config.buf, offset=config.offset, length=config.length):
    curvefs = swig_operate.LibCurve()
    fd = curvefs.libcurve_open(file_name, user_name, pass_word)
    logger.info("fd=%s, buf=%s, offset=%s, length=%s" % (fd, buf, offset, length))
    rs = curvefs.libcurve_write(fd, buf, offset, length)
    if rs < 0:
        logger.error("write_curve_file_for_snapshot libcurve_write file fail. rc = %s" % rs)
        return rs
        raise AssertionError
    rc = curvefs.libcurve_close(fd)
    if rc != 0:
        logger.info("write_curve_file_for_snapshot close libcurve file fail. rc = %s" % rc)
    return rc


def read_4k_length_curve_file():
    curvefs = swig_operate.LibCurve()
    fd = curvefs.libcurve_open(config.snapshot_file_name, config.user_name, config.pass_word)
    content = curvefs.libcurve_read(fd, "", 0, 4096)
    return content


def modify_curve_file_for_snapshot(file_name=config.snapshot_file_name, user_name=config.user_name,
                                   pass_word=config.pass_word, offset=config.offset, length=config.length):
    curvefs = swig_operate.LibCurve()
    fd = curvefs.libcurve_open(file_name, user_name, pass_word)
    buf = "tttttttt" * 512
    logger.info("fd=%s, buf=%s, offset=%s, length=%s" % (fd, buf, offset, length))
    rs = curvefs.libcurve_write(fd, buf, offset, length)
    if rs < 0:
        logger.error("write_curve_file_for_snapshot libcurve_write file fail. rc = %s" % rs)
        return rs
        raise AssertionError
    rc = curvefs.libcurve_close(fd)
    if rc != 0:
        logger.info("write_curve_file_for_snapshot close libcurve file fail. rc = %s" % rc)
    return rc


def snapshot_normal_create(file_name=config.snapshot_file_name, user_name=config.user_name, password=config.pass_word):
    client = snapshot_client.CurveSnapshot()
    seq = client.create_snapshot(file_name, user_name, password)
    logger.info("create_curve_file_for_snapshot file and return seq.value = %s" % seq)
    return seq


def snapshot_create_with_not_exist_file(file_name="/notexistfile", user_name=config.user_name,
                                        password=config.pass_word):
    client = snapshot_client.CurveSnapshot()
    rc = client.create_snapshot(file_name, user_name, password)
    logger.info("snapshot_create_with_not_exist_file , return rc = %s" % rc)
    if rc != 0:
        logger.error("snapshot_create_with_not_exist_file file fail. rc = %s" % rc)
    return rc


def snapshot_create_with_empty_str_file(file_name=" ", user_name=config.user_name,
                                        password=config.pass_word):
    client = snapshot_client.CurveSnapshot()
    rc = client.create_snapshot(file_name, user_name, password)
    logger.info("snapshot_create_with_empty_str_file , return rc = %s" % rc)
    if rc != 0:
        logger.error("snapshot_create_with_empty_str_file file fail. rc = %s" % rc)
    return rc


# "Special Characters`-=[];',./ ~!@#$%^&*()_+{}|:\"<>?"
# "Special   Characters`-=[]\\;',./ ~!@#$%^&*()_+{}|:\"<>?"
def snapshot_create_with_special_file_name(file_name="/特殊   字符`-=[]\\;',./ ~!@#$%^&*()_+{}|:\"<>?",
                                           user_name=config.user_name, password=config.pass_word):
    client = snapshot_client.CurveSnapshot()
    rc = client.create_snapshot(file_name, user_name, password)
    logger.info("snapshot_create_with_special_file_name , return rc = %s" % rc)
    if rc != 0:
        logger.error("snapshot_create_with_special_file_name file fail. rc = %s" % rc)
    return rc


def get_sanpshot_info(seq):
    client = snapshot_client.CurveSnapshot()
    finfo = client.get_snapshot(config.snapshot_file_name, config.user_name, config.pass_word, seq)
    # logger.info("get_sanpshot_info , file snapshot info.status = %s, owner = %s, filename = %s, "
    #             "length = %s, chunksize = %s, seqnum = %s, segmentsize = %s , parentid = %s, "
    #             "filetype = %s, ctime = %s" % (
    #                 finfo.filestatus, finfo.owner, finfo.filename, finfo.length.value, finfo.chunksize.value,
    #                 finfo.seqnum.value, finfo.segmentsize.value, finfo.parentid.value, finfo.filetype,
    #                 finfo.ctime.value))
    return finfo


# Create and obtain snapshot file information
def create_snapshot_and_get_snapshot_info(file_name=config.snapshot_file_name, user_name=config.user_name,
                                          password=config.pass_word):
    client = snapshot_client.CurveSnapshot()
    seq = client.create_snapshot(file_name, user_name, password)
    logger.info("create_snapshot_and_get_snapshot_info create snapshot success. seq = %s" % seq.value)
    finfo = client.get_snapshot(file_name, user_name, password, seq)
    return finfo


# Obtain snapshot file allocation information normally
def get_normal_snapshot_segment_info(file_name=config.snapshot_file_name, user_name=config.user_name,
                                     password=config.pass_word):
    seq = snapshot_normal_create(file_name, user_name, password)
    client = snapshot_client.CurveSnapshot()
    offset = curvesnapshot.type_uInt64_t()
    offset.value = 0
    seginfo = client.get_snapshot_SegmentInfo(file_name, user_name, password, seq, offset)
    logger.info("get_normal_snapshot_segment_info seq = %s, seginfo = %s" % seq % seginfo)
    return seginfo


def get_normal_chunk_info(file_name=config.snapshot_file_name, user_name=config.user_name, password=config.pass_word):
    seginfo = get_normal_snapshot_segment_info(file_name, user_name, password)
    logger.info("get_normal_chunkInfo segment info = %s" % seginfo)
    client = snapshot_client.CurveSnapshot()
    chunkinfo = client.get_chunk_Info(seginfo.chunkvec[0])
    logger.info("get_normal_chunkInfo chunkInfo info = %s" % chunkinfo)
    return chunkinfo  # Can perform assertion validation on chunInfo.chunkSn


def get_chunk_info_with_chunk_id_info(idinfo):
    client = snapshot_client.CurveSnapshot()
    chunkinfo = client.get_chunk_Info(idinfo)
    # logger.info(
    #     "get_chunk_info_with_chunk_id_info, chunk sn = %s, sn size = %s" % (chunkinfo.chunkSn[0], chunkinfo.snSize.value))
    return chunkinfo


def get_snapshot_first_segment_info(seq):
    client = snapshot_client.CurveSnapshot()
    offset = curvesnapshot.type_uInt64_t()
    offset.value = 0
    seginfo = client.get_snapshot_SegmentInfo(config.snapshot_file_name, config.user_name, config.pass_word, seq,
                                             offset)
    # logger.info(
    #     "get_snapshot_first_segment_info seq = %s, segmsize = %s, chunksize = %s, startoffset = %s, chunkvecsize = %s, "
    #     % (
    #         seq.value, seginfo.segmentsize.value, seginfo.chunksize.value, seginfo.startoffset.value,
    #         seginfo.chunkVecSize.value))
    return seginfo


def get_snapshot_segment_info_with_seq_and_offset(seq, offset):
    client = snapshot_client.CurveSnapshot()
    seginfo = client.get_snapshot_SegmentInfo(config.snapshot_file_name, config.user_name, config.pass_word, seq,
                                              offset)
    # logger.info("get_snapshot_segment_info_with_seq_and_offset seq = %s, segmsize = %s, chunksize = %s, startoffset = "
    #             "%s, chunkvecsize = %s" % (
    #                 seq.value, seginfo.segmentsize.value, seginfo.chunksize.value, seginfo.startoffset.value,
    #                 seginfo.chunkVecSize.value))
    return seginfo


def read_first_chunk_snapshot(seq):
    client = snapshot_client.CurveSnapshot()
    offset = curvesnapshot.type_uInt64_t()
    offset.value = 0
    seginfo = client.get_snapshot_SegmentInfo(config.snapshot_file_name, config.user_name, config.pass_word, seq,
                                              offset)
    logger.info(
        "get_snapshot_first_segment_info seq = %s, segmsize = %s, chunksize = %s, startoffset = %s, chunkvecsize = %s" % (
            seq.value, seginfo.segmentsize.value, seginfo.chunksize.value, seginfo.startoffset.value,
            seginfo.chunkVecSize.value))
    buf = read_chunk_snapshot(seginfo.chunkvec[0], seq)
    logger.info("read_first_chunk_snapshot buf = %s" % buf)
    return buf


def read_chunk_snapshot(idinfo, seq):
    client = snapshot_client.CurveSnapshot()
    offset = curvesnapshot.type_uInt64_t()
    offset.value = 0
    len = curvesnapshot.type_uInt64_t()
    len.value = 8 * 512
    # buf = "tttttttt" * 1024 * 1024 * 2 #16M
    buf = "tttttttt" * 512
    rc = client.read_chunk_snapshot(idinfo, seq, offset, len, buf)
    if rc != len.value:
        logger.info("read_chunk_snapshot fail , expect len = %s, real len = %s" % (len.value, rc))
        return rc
    logger.info("read_chunk_snapshot ,return buf = %s" % buf)
    return buf


def check_snapshot_status(seq):
    client = snapshot_client.CurveSnapshot()
    status = client.check_snapshot_status(config.snapshot_file_name, config.user_name, config.pass_word, seq)
    logger.info("check_snapshot_status rc = %s " % status)
    return status


def delete_file_snapshot(seq):
    client = snapshot_client.CurveSnapshot()
    rc = client.delete_snapshot(config.snapshot_file_name, config.user_name, config.pass_word, seq)
    return rc


def create_clone_chunk_with_s3_object(chunkidinfo):
    client = snapshot_client.CurveSnapshot()
    logger.info("create_clone_chunk_with_s3_object src chunkidinfo cid=%s, lpid=%s, cpid=%s" % (chunkidinfo.cid_.value,
                chunkidinfo.lpid_.value, chunkidinfo.cpid_.value))
    idinfo = curvesnapshot.CChunkIDInfo_t()
    idinfo.cid_.value = 1088
    idinfo.lpid_.value = chunkidinfo.lpid_.value
    idinfo.cpid_.value = chunkidinfo.cpid_.value
    chunksize = curvesnapshot.type_uInt64_t()
    chunksize.value = 16 * 1024 * 1024
    seq = curvesnapshot.type_uInt64_t()
    seq.value = 1
    correctseq = curvesnapshot.type_uInt64_t()
    correctseq.value = 0
    rc = client.create_clone_chunk(config.snapshot_s3_object_location, idinfo, seq, correctseq, chunksize)
    return rc


def delete_chunk_snapshot_with_correct_sn(chunkidinfo, correctseq):
    client = snapshot_client.CurveSnapshot()
    rc = client.delete_chunk_snapshot_or_correct_sn(chunkidinfo, correctseq)
    return rc


def recover_chunk_data(chunkidinfo):
    client = snapshot_client.CurveSnapshot()
    offset = curvesnapshot.type_uInt64_t()
    len = curvesnapshot.type_uInt64_t()
    offset.value = 0
    len.value = 1 * 1024 * 1024
    rc = client.recover_chunk(chunkidinfo, offset, len)
    return rc

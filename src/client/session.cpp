/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 4:58:20 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "proto/topology.pb.h"
#include "proto/nameserver2.pb.h"
#include "src/client/session.h"
#include "src/client/metacache.h"
#include "src/client/io_context_manager.h"
#include "src/client/request_scheduler.h"
#include "src/client/request_sender_manager.h"

DEFINE_uint32(queue_size,
                4096,
                "scheduler queue capacity!");
DEFINE_uint32(scheduler_threadnum,
                8,
                "scheduler thread pool thread nums!");
DEFINE_string(metaserver_addr,
                "127.0.0.1:6666",
                "meta server address ip:port");
DEFINE_uint32(chunk_size,
                16 * 1024 * 1024,
                "each chunk size.");
DEFINE_uint64(segment_size,
                1 * 1024 * 1024 * 1024,
                "each segment size will MDS allocate once.");

namespace curve {
namespace client {

    Session::Session() {
        segmentsize_ = FLAGS_segment_size;
        chunksize_ = FLAGS_chunk_size;
        mc_ = nullptr;
        ioctxManager_ = nullptr;
        mdschannel_ = nullptr;
        scheduler_ = nullptr;
        reqsenderManager_ = nullptr;
    }

    Session::~Session() {
    }

    bool Session::Initialize() {
        bool ret = false;
        do {
            /**
             * metacache should be Initialize before ioctxManager
             */
            mc_ = new (std::nothrow) MetaCache(this);
            if (CURVE_UNLIKELY(mc_ == nullptr)) {
                LOG(ERROR) << "allocate metacache failed!";
                break;
            }

            mdschannel_ = new (std::nothrow) brpc::Channel();
            if (CURVE_UNLIKELY(mdschannel_ == nullptr)) {
                LOG(ERROR) << "allocate mdschannel_ failed!";
                break;
            }

            if (mdschannel_->Init(FLAGS_metaserver_addr.c_str(),
                                nullptr) != 0) {
                LOG(ERROR) << "Init channel failed!";
                break;
            }

            reqsenderManager_ = new (std::nothrow) RequestSenderManager();
            if (CURVE_UNLIKELY(reqsenderManager_ == nullptr)) {
                LOG(ERROR) << "allocate RequestSenderManager failed!";
                break;
            }

            scheduler_ = new (std::nothrow) RequestScheduler();
            if (CURVE_UNLIKELY(scheduler_ == nullptr)) {
                LOG(ERROR) << "allocate RequestScheduler failed!";
                break;
            }

            ioctxManager_ = new (std::nothrow) IOContextManager(mc_, scheduler_);  //NOLINT
            if (CURVE_UNLIKELY(ioctxManager_ == nullptr)) {
                LOG(ERROR) << "allocate IOContextManager failed!";
                break;
            }

            if (!ioctxManager_->Initialize()) {
                LOG(ERROR) << "Init io context manager failed!";
                break;
            }

            if (-1 == scheduler_->Init(FLAGS_queue_size,
                                    FLAGS_scheduler_threadnum,
                                    reqsenderManager_,
                                    mc_)) {
                LOG(ERROR) << "Init scheduler_ failed!";
                break;
            }

            scheduler_->Run();

            ret = true;
        } while (0);

        if (!ret) {
            delete mc_;
            delete mdschannel_;
            delete ioctxManager_;
            delete scheduler_;
            delete reqsenderManager_;
        }
        return ret;
    }

    void Session::UnInitialize() {
        scheduler_->Fini();
        ioctxManager_->UnInitialize();
        delete mc_;
        delete mdschannel_;
        delete ioctxManager_;
        delete scheduler_;
        delete reqsenderManager_;
        mc_ = nullptr;
        mdschannel_ = nullptr;
        ioctxManager_ = nullptr;
        scheduler_ = nullptr;
        reqsenderManager_ = nullptr;
    }

    CreateFileErrorType Session::CreateFile(std::string filename, size_t size) {
        // init client
        brpc::Channel channel;
        if (channel.Init(FLAGS_metaserver_addr.c_str(), nullptr) != 0) {
            LOG(FATAL) << "Init channel  failed!";
            return CreateFileErrorType::FILE_CREATE_FAILED;
        }
        curve::mds::CurveFSService_Stub stub(&channel);

        // CreateFile
        curve::mds::CreateFileRequest request;
        curve::mds::CreateFileResponse response;
        brpc::Controller cntl;
        uint64_t fileLength = size;

        request.set_filename(filename);
        request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        request.set_filelength(fileLength);

        cntl.set_log_id(1);  // TODO(tongguangxun) : specify the log id usage
        stub.CreateFile(&cntl, &request, &response, NULL);

        if (cntl.Failed()) {
            LOG(ERROR) << "Create file failed, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText();
        }

        if (response.has_statuscode()) {
            if (response.statuscode() == curve::mds::StatusCode::kFileExists) {
                return CreateFileErrorType::FILE_ALREADY_EXISTS;
            } else if (response.statuscode() == curve::mds::StatusCode::kOK) {
                return CreateFileErrorType::FILE_CREATE_OK;
            }
        }
        return CreateFileErrorType::FILE_CREATE_FAILED;
    }

    OpenFileErrorType Session::Open(std::string filename) {
        FInfo_t fi;
        if (curve::mds::StatusCode::kOK
                == GetFileInfo(filename, &fi)) {
            filename_ = filename;
            return OpenFileErrorType::FILE_OPEN_OK;
        }
        return OpenFileErrorType::FILE_OPEN_FAILED;
    }

    int Session::GetFileInfo(std::string filename, FInfo_t* fi) {
         // init client
         // TODO(tongguangxun): we need process multi metaserver case,
         // for the mds will be a raft group
        brpc::Channel channel;
        if (channel.Init(FLAGS_metaserver_addr.c_str(), nullptr) != 0) {
            LOG(FATAL) << "Init channel  failed!";
            return -1;
        }
        curve::mds::CurveFSService_Stub stub(&channel);

        curve::mds::GetFileInfoRequest request;
        curve::mds::GetFileInfoResponse response;
        brpc::Controller cntl;
        request.set_filename(filename);
        stub.GetFileInfo(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            LOG(ERROR) << "get file info failed, errcorde = "
                        << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText();
        } else {
            if (response.has_fileinfo()) {
                curve::mds::FileInfo finfo = response.fileinfo();
                if (finfo.has_filename()) {
                    memcpy(fi->filename,
                            finfo.filename().c_str(),
                            finfo.filename().size());
                    fi->filename[finfo.filename().size()] = '\0';
                }
                if (finfo.has_id()) {
                    fi->id = finfo.id();
                }
                if (finfo.has_parentid()) {
                    fi->parentid = finfo.parentid();
                }
                if (finfo.has_filetype()) {
                    fi->filetype = static_cast<FileType>(finfo.filetype());
                }
                if (finfo.has_chunksize()) {
                    fi->chunksize = finfo.chunksize();
                }
                if (finfo.has_length()) {
                    fi->length = finfo.length();
                }
                if (finfo.has_ctime()) {
                    fi->ctime = finfo.ctime();
                }
                if (finfo.has_chunksize()) {
                    fi->chunksize = finfo.chunksize();
                }
                if (finfo.has_snapshotid()) {
                    fi->snapshotid = finfo.snapshotid();
                }
                if (finfo.has_segmentsize()) {
                    fi->segmentsize = finfo.segmentsize();
                }
            }
        }
        if (response.has_statuscode()) {
            return response.statuscode();
        }
        return -1;
    }

    int Session::GetOrAllocateSegment(off_t offset) {
        brpc::Controller cntl;

        curve::mds::GetOrAllocateSegmentRequest request;
        curve::mds::GetOrAllocateSegmentResponse response;

        curve::mds::CurveFSService_Stub stub(mdschannel_);

        /**
         * convert the user offset to seg  offset
         */
        uint64_t seg_offset = (offset / FLAGS_segment_size)
                              * FLAGS_segment_size;
        request.set_filename(filename_);
        request.set_offset(seg_offset);
        request.set_allocateifnotexist(true);

        stub.GetOrAllocateSegment(&cntl, &request, &response, NULL);
        DVLOG(9) << "Get segment at offset: " << seg_offset
            << "Response status: " << response.statuscode();

        std::vector<CopysetID> csids;
        csids.clear();
        if (cntl.Failed()) {
            LOG(ERROR) << "allocate segment failed, error code = "
                        << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText();
        } else {
            LogicPoolID logicpoolid = 0;
            uint64_t startoffset = 0;
            curve::mds::PageFileSegment pfs;
            if (response.has_pagefilesegment()) {
                pfs = response.pagefilesegment();
                if (pfs.has_logicalpoolid()) {
                    logicpoolid = pfs.logicalpoolid();
                }
                if (pfs.has_segmentsize()) {
                    segmentsize_ = pfs.segmentsize();
                    FLAGS_segment_size = segmentsize_;
                }
                if (pfs.has_chunksize()) {
                    chunksize_ = pfs.chunksize();
                    FLAGS_chunk_size = chunksize_;
                }
                if (pfs.has_startoffset()) {
                    startoffset = pfs.startoffset();
                }

                int chunksNum = pfs.chunks_size();
                if (chunksNum == 0) {
                    LOG(ERROR) << "mds allocate segment, but no chunk info!";
                    return -1;
                }
                DVLOG(9) << "update meta cache of " << chunksNum << " chunks"
                         << " at offset: " << startoffset;
                for (int i = 0; i < chunksNum; i++) {
                    ChunkID chunkid = 0;
                    CopysetID copysetid = 0;
                    if (pfs.chunks(i).has_chunkid()) {
                        chunkid = pfs.chunks(i).chunkid();
                    }
                    if (pfs.chunks(i).has_copysetid()) {
                        copysetid = pfs.chunks(i).copysetid();
                    }

                    csids.push_back(copysetid);
                    ChunkIndex cindex = (startoffset + i * chunksize_)
                                         / chunksize_;
                    Chunkinfo_t chunkinfo;
                    chunkinfo.logicpoolid_ = logicpoolid;
                    chunkinfo.copysetid_ = copysetid;
                    chunkinfo.chunkid_ = chunkid;
                    mc_->UpdateChunkInfo(cindex, chunkinfo);
                    mc_->UpdateCopysetIDInfo(chunkid, logicpoolid, copysetid);
                    DVLOG(9) << "chunk id: " << i
                             << " pool id: " << logicpoolid
                             << " copyset id: " << copysetid
                             << " cindex: " << cindex
                             << " chunk id: " << chunkid;
                }
            }
            // now get the segment copyset server addr
            return GetServerList(logicpoolid, csids);
        }
        return -1;
    }

    int Session::GetServerList(LogicPoolID lpid,
                            const std::vector<CopysetID>& csid) {
        brpc::Controller cntl;

        curve::mds::topology::GetChunkServerListInCopySetsRequest request;
        curve::mds::topology::GetChunkServerListInCopySetsResponse response;

        curve::mds::topology::TopologyService_Stub stub(mdschannel_);

        request.set_logicalpoolid(lpid);
        for (auto iter : csid) {
            request.add_copysetid(iter);
        }

        stub.GetChunkServerListInCopySets(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            LOG(ERROR) << "get server list from mds failed, status code = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText();
        } else {
            int csinfonum = response.csinfo_size();
            for (int i = 0; i < csinfonum; i++) {
                curve::mds::topology::CopySetServerInfo info = response.csinfo(i);  //NOLINT
                CopysetID csid = 0;
                if (info.has_copysetid()) {
                    csid = info.copysetid();
                }
                CopysetInfo_t copysetseverl;

                int cslocsNum = info.cslocs_size();
                for (int j = 0; j < cslocsNum; j++) {
                    curve::mds::topology::ChunkServerLocation csl = info.cslocs(j); //NOLINT
                    CopysetPeerInfo_t csinfo;
                    std::string hostip;
                    uint16_t port;
                    if (csl.has_chunkserverid()) {
                        csinfo.chunkserverid_ = csl.chunkserverid();
                    }
                    if (csl.has_hostip()) {
                        hostip = csl.hostip();
                    }
                    if (csl.has_port()) {
                        port = csl.port();
                    }
                    EndPoint ep;
                    butil::str2endpoint(hostip.c_str(), port, &ep);
                    PeerId pd(ep);
                    csinfo.peerid_ = pd;
                    copysetseverl.AddCopysetPeerInfo(csinfo);
                }
                mc_->UpdateCopysetInfo(lpid, csid, copysetseverl);
            }
        }
        if (response.has_statuscode()) {
            return response.statuscode();
        }
        return -1;
    }

}   // namespace client
}   // namespace curve

/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Friday September 7th 2018
 * Author: hzsunjianliang
 */

#ifndef SRC_MDS_NAMESERVER2_CURVEFS_H_
#define SRC_MDS_NAMESERVER2_CURVEFS_H_

#include <bvar/bvar.h>
#include <vector>
#include <string>
#include <memory>
#include <thread>  //NOLINT
#include <chrono>
#include "proto/nameserver2.pb.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/chunk_allocator.h"
#include "src/mds/nameserver2/clean_manager.h"
#include "src/mds/nameserver2/async_delete_snapshot_entity.h"
#include "src/mds/nameserver2/file_record.h"
#include "src/mds/nameserver2/idgenerator/inode_id_generator.h"
#include "src/mds/dao/mdsRepo.h"
#include "src/common/authenticator.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"
using curve::common::Authenticator;

namespace curve {
namespace mds {

struct RootAuthOption {
    std::string rootOwner;
    std::string rootPassword;
};

struct CurveFSOption {
    uint64_t defaultChunkSize;
    RootAuthOption authOptions;
    FileRecordOptions fileRecordOptions;
};

struct AllocatedSize {
    // mds给文件分配的segment的大小
    uint64_t allocatedSize;
    // 实际会占用的底层空间
    uint64_t physicalAllocatedSize;
    AllocatedSize() : allocatedSize(0), physicalAllocatedSize(0) {}
    AllocatedSize& operator+=(const AllocatedSize& rhs);
};

using ::curve::mds::DeleteSnapShotResponse;

class CurveFS {
 public:
    // singleton, supported in c++11
    static CurveFS &GetInstance() {
        static CurveFS curvefs;
        return curvefs;
    }

    /**
     *  @brief CurveFS初始化
     *  @param NameServerStorage:
     *         InodeIDGenerator：
     *         ChunkSegmentAllocator：
     *         CleanManagerInterface:
     *         fileRecordManager
     *         allocStatistic: 分配统计模块
     *         CurveFSOption : 对curvefs进行初始化需要的参数
     *         repo : curvefs持久化数据所用的数据库，目前保存client注册信息使用
     *  @return 初始化是否成功
     */
    bool Init(std::shared_ptr<NameServerStorage>,
              std::shared_ptr<InodeIDGenerator>,
              std::shared_ptr<ChunkSegmentAllocator>,
              std::shared_ptr<CleanManagerInterface>,
              std::shared_ptr<FileRecordManager> fileRecordManager,
              std::shared_ptr<AllocStatistic> allocStatistic,
              const struct CurveFSOption &curveFSOptions,
              std::shared_ptr<MdsRepo> repo,
              std::shared_ptr<Topology> topology);

    /**
     *  @brief Run session manager
     *  @param
     *  @return
     */
    void Run();

    /**
     *  @brief CurveFS Uninit
     *  @param
     *  @return
     */
    void Uninit();

    // namespace ops
    /**
     *  @brief 创建文件
     *  @param fileName: 文件名
     *         owner: 文件的拥有者
     *         filetype：文件类型
     *         length：文件长度
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode CreateFile(const std::string & fileName,
                          const std::string& owner,
                          FileType filetype,
                          uint64_t length);
    /**
     *  @brief 获取文件信息。
     *         如果文件不存在返回StatusCode::kFileNotExists
     *         如果文件元数据获取错误，返回StatusCode::kStorageError
     *  @param filename：文件名
     *         inode：返回获取到的文件系统
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode GetFileInfo(const std::string & filename,
                           FileInfo * inode) const;

     /**
     *  @brief 获取分配大小
     *  @param: fileName：文件名
     *  @param[out]: allocatedSize： 文件或目录的分配大小
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode GetAllocatedSize(const std::string& fileName,
                                AllocatedSize* allocatedSize);

    /**
     *  @brief 删除文件
     *  @param[in] filename:文件名
     *  @param[in] fileId：文件inodeid，对删除的文件进行inodeid校验，
     *                 如果传入的fileId为kUnitializedFileID，则不校验
     *  @param[in] deleteForce:是否强制删除，在默认情况下删除进入回收站，
     *                 root用户可以选择将文件强制删除,当前目录不支持进入回收站
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode DeleteFile(const std::string & filename, uint64_t fileId,
        bool deleteForce = false);

    /**
     *  @brief 获取目录下所有文件信息
     *  @param dirname：目录名字
     *         files：返回查到的结果
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode ReadDir(const std::string & dirname,
                       std::vector<FileInfo> * files) const;

    /**
     *  @brief 重命名文件
     *  @param oldFileName：旧文件名
     *         newFileName：希望重命名的新文件名
     *         oldFileId：旧文件inodeid，对删除的文件进行inodeid校验，
     *                    如果传入的fileId为kUnitializedFileID，则不校验
     *         newFileId：新文件inodeid，对删除的文件进行inodeid校验，
     *                    如果传入的fileId为kUnitializedFileID，则不校验
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    // TODO(hzsunjianliang): 添加源文件的inode的参数，用于检查
    StatusCode RenameFile(const std::string & oldFileName,
                          const std::string & newFileName,
                          uint64_t oldFileId,
                          uint64_t newFileId);

    /**
     *  @brief 扩容文件
     *  @param filename：文件名
     *         newSize：希望扩容后的文件大小
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    // extent size minimum unit 1GB ( segement as a unit)
    StatusCode ExtendFile(const std::string &filename,
                          uint64_t newSize);

    /**
     *  @brief 修改文件owner信息
     *  @param fileName: 文件名
               newOwner：希望文件owner变更后的新的owner
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode ChangeOwner(const std::string &filename,
                           const std::string &newOwner);

    // segment(chunk) ops
    /**
     *  @brief 查询segment信息，如果segment不存在，根据allocateIfNoExist决定是否
     *         创建新的segment
     *  @param filename：文件名
     *         offset: segment的偏移
     *         allocateIfNoExist：如果segment不存在，是否需要创建新的segment
     *         segment：返回查询到的segment信息
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode GetOrAllocateSegment(
        const std::string & filename,
        offset_t offset,
        bool allocateIfNoExist, PageFileSegment *segment);

    /**
     *  @brief 获取root文件信息
     *  @param
     *  @return 返回获取到的root文件信息
     */
    FileInfo GetRootFileInfo(void) const {
        return rootFileInfo_;
    }

    /**
     *  @brief 创建快照，如果创建成功，返回创建的快照文件info
     *  @param filename：文件名
     *         snapshotFileInfo: 返回创建的快照文件info
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode CreateSnapShotFile(const std::string &fileName,
                            FileInfo *snapshotFileInfo);

    /**
     *  @brief 获取文件的所有快照info
     *  @param filename：文件名
     *         snapshotFileInfos: 返回文件的所有的快照info
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode ListSnapShotFile(const std::string & fileName,
                            std::vector<FileInfo> *snapshotFileInfos) const;
    // async interface
    /**
     *  @brief 删除快照文件，删除文件指定seq的快照文件
     *  @param filename：文件名
     *         seq: 快照的seq
     *         entity: 异步删除快照entity
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode DeleteFileSnapShotFile(const std::string &fileName,
                            FileSeqType seq,
                            std::shared_ptr<AsyncDeleteSnapShotEntity> entity);

    /**
     *  @brief 获取快照的状态，如果状态是kFileDeleting，额外返回删除进度
     *  @param fileName : 文件名
     *         seq : 快照的sequence
     *         [out] status : 文件状态 kFileCreated, kFileDeleting
     *         [out] progress : 如果状态是kFileDeleting，这个参数额外返回删除进度
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode CheckSnapShotFileStatus(const std::string &fileName,
                            FileSeqType seq, FileStatus * status,
                            uint32_t * progress) const;

    /**
     *  @brief 获取快照info信息
     *  @param filename：文件名
     *         seq: 快照的seq
     *         snapshotFileInfo: 返回查询到的快照信息
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode GetSnapShotFileInfo(const std::string &fileName,
                            FileSeqType seq, FileInfo *snapshotFileInfo) const;

    /**
     *  @brief 获取快照的segment信息
     *  @param filename：文件名
     *         seq: 快照的seq
     *         offset：
     *         segment: 返回查询到的快照segment信息
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode GetSnapShotFileSegment(
            const std::string & filename,
            FileSeqType seq,
            offset_t offset,
            PageFileSegment *segment);

    // session ops
    /**
     *  @brief 打开文件
     *  @param filename：文件名
     *         clientIP：clientIP
     *         session：返回创建的session信息
     *         fileInfo：返回打开的文件信息
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode OpenFile(const std::string &fileName,
                        const std::string &clientIP,
                        ProtoSession *protoSession,
                        FileInfo  *fileInfo);

    /**
     *  @brief 关闭文件
     *  @param fileName: 文件名
     *         sessionID：sessionID
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode CloseFile(const std::string &fileName,
                         const std::string &sessionID);

    /**
     *  @brief 更新session的有效期
     *  @param filename：文件名
     *         sessionid：sessionID
     *         date: 请求的时间，用来防止重放攻击
     *         signature: 用来进行请求的身份验证
     *         clientIP: clientIP
     *         fileInfo: 返回打开的文件信息
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode RefreshSession(const std::string &filename,
                              const std::string &sessionid,
                              const uint64_t date,
                              const std::string &signature,
                              const std::string &clientIP,
                              uint32_t clientPort,
                              const std::string &clientVersion,
                              FileInfo  *fileInfo);

    /**
     * @breif 创建克隆文件，当前克隆文件的创建只有root用户能够创建
     * @param filename 文件名
     * @param owner 调用接口的owner信息
     * @param filetype 文件的类型
     * @param length 克隆文件的长度
     * @param seq 版本号
     * @param ChunkSizeType 创建克隆文件的chunk大小
     * @param cloneSource 克隆源文件地址，当前只支持curvefs
     * @param cloneLength 克隆源文件长度
     * @param[out] fileInfo 创建成功克隆文件的fileInfo
     * @return 成功返回StatusCode:kOK
     */
    StatusCode CreateCloneFile(const std::string &filename,
                            const std::string& owner,
                            FileType filetype,
                            uint64_t length,
                            FileSeqType seq,
                            ChunkSizeType chunksize,
                            FileInfo *fileInfo,
                            const std::string & cloneSource = "",
                            uint64_t cloneLength = 0);

    /**
     * @brief 设置克隆文件的状态
     * @param filename 文件名
     * @param fileID 设置文件的inodeid
     * @param fileStatus 需要设置的状态
     *
     * @return  是否成功，成功返回StatusCode::kOK
     *
     */
    StatusCode SetCloneFileStatus(const std::string &filename,
                            uint64_t fileID,
                            FileStatus fileStatus);
    /**
     *  @brief 检查的文件owner
     *  @param: filename：文件名
     *  @param: owner：文件的拥有者
     *  @param: signature是用户侧传过来的签名信息
     *  @param: date是用于计算signature的时间
     *  @return 是否成功，成功返回StatusCode::kOK
     *          验证失败返回StatusCode::kOwnerAuthFail
     *          其他失败
     */
    StatusCode CheckFileOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date);

    /**
     *  @brief 检查的文件各级目录的owner
     *  @param: filename：文件名
     *  @param: owner：文件的拥有者
     *  @param: signature是用户侧传过来的签名信息
     *  @param: date是用于计算signature的时间
     *  @return 是否成功，成功返回StatusCode::kOK
     *          验证失败返回StatusCode::kOwnerAuthFail
     *          其他失败，kFileNotExists，kStorageError，kNotDirectory
     */
    StatusCode CheckPathOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date);

    /**
     *  @brief 检查的RenameNewfile文件各级目录的owner
     *  @param: filename：文件名
     *  @param: owner：文件的拥有者
     *  @param: signature是用户侧传过来的签名信息
     *  @param: date是用于计算signature的时间
     *  @return 是否成功，成功返回StatusCode::kOK
     *          验证失败返回StatusCode::kOwnerAuthFail
     *          其他失败，kFileNotExists，kStorageError，kNotDirectory
     */
    StatusCode CheckDestinationOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date);

    /**
     *  @brief 检查的文件的owner，owner必须是root用户
     *  @param: filename：文件名
     *  @param: owner：文件的拥有者
     *  @param: signature是用户侧传过来的签名信息
     *  @param: date是用于计算signature的时间
     *  @return 是否成功，成功返回StatusCode::kOK
     *          验证失败返回StatusCode::kOwnerAuthFail
     *          其他失败，kFileNotExists，kStorageError，kNotDirectory
     */
    StatusCode CheckRootOwner(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              uint64_t date);

    /**
     *  @brief 注册client信息
     *  @param: ip：client的ip信息
     *  @param: port：client的端口信息
     *  @return 是否成功，成功返回StatusCode::kOK
     *          失败返回StatusCode::KInternalError
     */
    StatusCode RegistClient(const std::string &ip, uint32_t port);

    /**
     *  @brief 获取fileRecord中的client的信息
     *  @param listAllClient 是否列出所有client信息
     *  @param[out]:  client信息的列表
     *  @return 是否成功，成功返回StatusCode::kOK
     *          失败返回StatusCode::KInternalError
     */
    StatusCode ListClient(bool listAllClient,
                          std::vector<ClientInfo>* clientInfos);

    /**
     * @brief 查询文件的挂载点
     * @param clientInfo 文件被挂载的节点信息
     * @return 是否成功，成功返回StatusCode::kOK
     *         失败返回 StatusCode::kFileNotExists
     */
    StatusCode FindFileMountPoint(const std::string& fileName,
                                  ClientInfo* clientInfo);

    /**
     *  @brief 获取已经open的文件个数
     *  @param:
     *  @return 如果curvefs未初始化，返回0
     */
    uint64_t GetOpenFileNum();

    /**
     *  @brief 获取curvefs的defaultChunkSize信息
     *  @param:
     *  @return 返回获取的defaultChunkSize信息
     */
    uint64_t GetDefaultChunkSize();

 private:
    CurveFS() = default;

    void InitRootFile(void);

    bool InitRecycleBinDir();

    StatusCode WalkPath(const std::string &fileName,
                        FileInfo *fileInfo, std::string  *lastEntry) const;

    StatusCode LookUpFile(const FileInfo & parentFileInfo,
                          const std::string & fileName,
                          FileInfo *fileInfo) const;

    StatusCode PutFile(const FileInfo & fileInfo);

    /**
     * @brief 执行一次fileinfo的snapshot快照事务
     * @param originalFileInfo: 原文件对于fileInfo
     * @param SnapShotFile: 生成的snapshot文件对于fileInfo
     * @return StatusCode: 成功或者失败
     */
    StatusCode SnapShotFile(const FileInfo * originalFileInfo,
        const FileInfo * SnapShotFile) const;

    std::string GetRootOwner() {
        return rootAuthOptions_.rootOwner;
    }

    /**
     * @brief: 检查当前请求date是否合法，与当前时间前后15分钟内为合法
     * @param: date请求的时间点
     * @return: 合法返回true，否则false
     */
    bool CheckDate(uint64_t date);
    /**
     *  @brief 检查请求的signature是否合法
     *  @param: owner：文件的拥有者
     *  @param: signature是用户侧传过来的签名信息
     *  @param: date是用于计算signature的时间
     *  @return: 签名合法为true，否则false
     */
    bool CheckSignature(const std::string& owner,
                        const std::string& signature,
                        uint64_t date);

    StatusCode CheckPathOwnerInternal(const std::string &filename,
                              const std::string &owner,
                              const std::string &signature,
                              std::string *lastEntry,
                              uint64_t *parentID);

    /**
     *  @brief 判断一个目录下是否没有文件
     *  @param: fileInfo：目录的fileInfo
     *  @param: result: 目录下是否是空的，true表示目录为空，false表示目录非空
     *  @return: 是否成功，成功返回StatusCode::kOK
     */
    StatusCode isDirectoryEmpty(const FileInfo &fileInfo, bool *result);

    /**
     * @brief 当前是否允许打快照
     *        允许打快照的情况:
     *        1.filerecord记录的版本号>="0.0.6" 2.没有filerecord记录
     *        不允许打快照的情况: filerecord记录的版本号为空或者小于"0.0.6"
     *
     * @param fileName 文件名
     *
     * @return 返回值有三种：
     *         StatusCode::kOK 允许打快照
     *         StatusCode::kSnapshotFrozen snapshot功能为启用
     *         StatusCode::kClientVersionNotMatch client版本不允许打快照
     */
    StatusCode IsSnapshotAllowed(const std::string &fileName);

    /**
     *  @brief 判断文件是否进行更改，目前删除、rename、changeowner时需要判断
     *  @param: fileName
     *  @param: fileInfo 文件信息结构
     *  @return: 正常返回kOK，否则返回错误码
     */
    StatusCode CheckFileCanChange(const std::string &fileName,
        const FileInfo &fileInfo);

    /**
     *  @brief 获取分配大小
     *  @param: fileName：文件名
     *  @param: fileInfo 文件信息
     *  @param[out]: allocSize： 文件或目录的分配大小
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode GetAllocatedSize(const std::string& fileName,
                                const FileInfo& fileInfo,
                                AllocatedSize* allocSize);

    /**
     *  @brief 获取文件分配大小
     *  @param: fileName：文件名
     *  @param: fileInfo 文件信息
     *  @param[out]: allocSize： 文件的分配大小
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode GetFileAllocSize(const std::string& fileName,
                                const FileInfo& fileInfo,
                                AllocatedSize* allocSize);

    /**
     *  @brief 获取目录分配大小
     *  @param: dirName：目录名
     *  @param: fileInfo 文件信息
     *  @param[out]: allocSize： 目录的分配大小
     *  @return 是否成功，成功返回StatusCode::kOK
     */
    StatusCode GetDirAllocSize(const std::string& fileName,
                                const FileInfo& fileInfo,
                                AllocatedSize* allocSize);

 private:
    FileInfo rootFileInfo_;
    std::shared_ptr<NameServerStorage> storage_;
    std::shared_ptr<InodeIDGenerator> InodeIDGenerator_;
    std::shared_ptr<ChunkSegmentAllocator> chunkSegAllocator_;
    std::shared_ptr<FileRecordManager> fileRecordManager_;
    std::shared_ptr<CleanManagerInterface> cleanManager_;
    std::shared_ptr<AllocStatistic> allocStatistic_;
    std::shared_ptr<Topology> topology_;
    struct RootAuthOption       rootAuthOptions_;

    uint64_t defaultChunkSize_;
    std::shared_ptr<MdsRepo> repo_;
    std::chrono::steady_clock::time_point startTime_;
};
extern CurveFS &kCurveFS;
}   // namespace mds
}   // namespace curve
#endif   // SRC_MDS_NAMESERVER2_CURVEFS_H_


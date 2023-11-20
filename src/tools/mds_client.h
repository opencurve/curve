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
 * Created Date: 2019-11-25
 * Author: charisu
 */

#ifndef SRC_TOOLS_MDS_CLIENT_H_
#define SRC_TOOLS_MDS_CLIENT_H_

#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <json/json.h>

#include <vector>
#include <iostream>
#include <string>
#include <map>
#include <memory>
#include <unordered_map>

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "proto/schedule.pb.h"
#include "src/common/authenticator.h"
#include "src/mds/common/mds_define.h"
#include "src/common/string_util.h"
#include "src/common/timeutility.h"
#include "src/common/net_common.h"
#include "src/tools/metric_name.h"
#include "src/tools/metric_client.h"
#include "src/tools/common.h"
#include "src/tools/curve_tool_define.h"

using curve::common::ChunkServerLocation;
using curve::common::CopysetInfo;
using curve::mds::FileInfo;
using curve::mds::PageFileChunkInfo;
using curve::mds::PageFileSegment;
using curve::mds::StatusCode;
using curve::mds::topology::ChunkFormatStatus;
using curve::mds::topology::ChunkServerIdType;
using curve::mds::topology::ChunkServerInfo;
using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::CopySetIdType;
using curve::mds::topology::CopySetServerInfo;
using curve::mds::topology::GetChunkServerInfoRequest;
using curve::mds::topology::GetCopySetsInChunkServerRequest;
using curve::mds::topology::kTopoErrCodeSuccess;
using curve::mds::topology::ListChunkFormatStatusRequest;
using curve::mds::topology::ListChunkServerRequest;
using curve::mds::topology::LogicalPoolInfo;
using curve::mds::topology::PhysicalPoolInfo;
using curve::mds::topology::PoolIdType;
using curve::mds::topology::ServerIdType;
using curve::mds::topology::ServerInfo;
using curve::mds::topology::ZoneIdType;
using curve::mds::topology::ZoneInfo;

using curve::common::Authenticator;
using curve::mds::schedule::RapidLeaderScheduleRequst;
using curve::mds::schedule::RapidLeaderScheduleResponse;

namespace curve
{
    namespace tool
    {

        using curve::mds::topology::PoolsetInfo;

        enum class GetSegmentRes
        {
            kOK = 0,                   // Successfully obtained segment
            kSegmentNotAllocated = -1, // segment does not exist
            kFileNotExists = -2,       // File does not exist
            kOtherError = -3           // Other errors
        };

        using AllocMap = std::unordered_map<PoolIdType, uint64_t>;

        struct CreateFileContext
        {
            curve::mds::FileType type;
            std::string name;
            uint64_t length;
            uint64_t stripeUnit;
            uint64_t stripeCount;
            std::string poolset;
        };

        class MDSClient
        {
        public:
            MDSClient()
                : currentMdsIndex_(0), userName_(""), password_(""), isInited_(false) {}
            virtual ~MDSClient() = default;

            /**
             * @brief Initialize channel
             * @param mdsAddr Address of mds, supporting multiple addresses separated by
             * ','
             * @return returns 0 for success, -1 for failure
             */
            virtual int Init(const std::string &mdsAddr);

            /**
             * @brief Initialize channel
             * @param mdsAddr Address of mds, supporting multiple addresses separated by
             * ','
             * @param dummyPort dummy port list, if only one is entered
             *                  All mds use the same dummy port, separated by strings if
             * there are multiple Set different dummy ports for each mds
             * @return returns 0 for success, -1 for failure
             */
            virtual int Init(const std::string &mdsAddr, const std::string &dummyPort);

            /**
             * @brief Get file fileInfo
             * @param fileName File name
             * @param[out] fileInfo file fileInfo, valid when the return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int GetFileInfo(const std::string &fileName, FileInfo *fileInfo);

            /**
             * @brief Get file or directory allocation size
             * @param fileName File name
             * @param[out] allocSize file or directory allocation size, valid when the
             * return value is 0
             * @param[out] allocMap Allocation of files in various pools
             * @return returns 0 for success, -1 for failure
             */
            virtual int GetAllocatedSize(const std::string &fileName,
                                         uint64_t *allocSize,
                                         AllocMap *allocMap = nullptr);

            /**
             * @brief Get the size of a file or directory
             * @param fileName File name
             * @param[out] fileSize File or directory allocation size, valid when the
             * return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int GetFileSize(const std::string &fileName, uint64_t *fileSize);

            /**
             * @brief List all fileInfo in the directory
             * @param dirName directory name
             * @param[out] files All fileInfo files in the directory are valid when the
             * return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int ListDir(const std::string &dirName,
                                std::vector<FileInfo> *files);

            /**
             * @brief Get the segment with the specified offset and place it in the
             * segment
             * @param fileName File name
             * @param offset offset value
             * @param[out] segment The segmentInfo of the specified offset in the file
             * is valid when the return value is 0
             * @return returns GetSegmentRes, distinguishing between unassigned segments
             * and other errors
             */
            virtual GetSegmentRes GetSegmentInfo(const std::string &fileName,
                                                 uint64_t offset,
                                                 PageFileSegment *segment);

            /**
             * @brief Delete file
             * @param fileName File name
             * @param forcedelete: Do you want to force deletion
             * @return returns 0 for success, -1 for failure
             */
            virtual int DeleteFile(const std::string &fileName,
                                   bool forcedelete = false);

            /**
             * @brief create pageFile or directory
             * @param fileName file name or dir name
             * @param length File length
             * @param normalFile is file or dir
             * @param stripeUnit stripe unit size
             * @param stripeCount the amount of stripes
             * @return returns 0 for success, -1 for failure
             */
            virtual int CreateFile(const CreateFileContext &context);

            /**
             *  @brief List all volumes on copysets
             *  @param copysets
             *  @param[out] fileNames volumes name
             *  @return return 0 when success, -1 when fail
             */
            virtual int ListVolumesOnCopyset(
                const std::vector<common::CopysetInfo> &copysets,
                std::vector<std::string> *fileNames);

            /**
             * @brief expansion volume
             * @param fileName File name
             * @param newSize The volume size after expansion
             * @return returns 0 for success, -1 for failure
             */
            virtual int ExtendVolume(const std::string &fileName, uint64_t newSize);

            /**
             * @brief List the address of the client's dummyserver
             * @param[out] clientAddrs client address list, valid when 0 is returned
             * @param[out] listClientsInRepo also lists the clients in the database
             * @return returns 0 for success, -1 for failure
             */
            virtual int ListClient(std::vector<std::string> *clientAddrs,
                                   bool listClientsInRepo = false);

            /**
             * @brief Get the list of chunkservers in the copyset
             * @param logicalPoolId Logical Pool id
             * @param copysetId copyset id
             * @param[out] csLocs List of chunkserver locations, valid when the return
             * value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int GetChunkServerListInCopySet(
                const PoolIdType &logicalPoolId, const CopySetIdType &copysetId,
                std::vector<ChunkServerLocation> *csLocs);

            /**
             * @brief Get the list of chunkservers in the copyset
             * @param logicalPoolId Logical Pool ID
             * @param copysetIds List of copysetIds to query
             * @param[out] csServerInfos A list of  copyset members, valid when the
             * return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int GetChunkServerListInCopySets(
                const PoolIdType &logicalPoolId,
                const std::vector<CopySetIdType> &copysetIds,
                std::vector<CopySetServerInfo> *csServerInfos);

            /**
             * @brief Get a list of physical pools in the cluster
             * @param[out] pools A list of physical pool information, valid when the
             * return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int ListPhysicalPoolsInCluster(
                std::vector<PhysicalPoolInfo> *pools);

            /**
             * @brief Get a list of logical pools in the physical pool
             * @param id Physical pool id
             * @param[out] pools List of logical pool information, valid when the return
             * value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int ListLogicalPoolsInPhysicalPool(
                const PoolIdType &id, std::vector<LogicalPoolInfo> *pools);

            /**
             *List of logical pools in the  @brief cluster
             * @param[out] pools List of logical pool information, valid when the return
             *value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int ListLogicalPoolsInCluster(std::vector<LogicalPoolInfo> *pools);

            /**
             * @brief to obtain a list of zones in the physical pool
             * @param id Physical pool id
             * @param[out] zones A list of zone information, valid when the return value
             * is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int ListZoneInPhysicalPool(const PoolIdType &id,
                                               std::vector<ZoneInfo> *zones);

            /**
             * @brief to obtain a list of servers in the zone
             * @param id zone id
             * @param[out] servers List of server information, valid when the return
             * value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int ListServersInZone(const ZoneIdType &id,
                                          std::vector<ServerInfo> *servers);

            /**
             * @brief Get a list of chunkservers on the server
             * @param id server id
             * @param[out] chunkservers A list of chunkserver information, valid when
             * the return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int ListChunkServersOnServer(
                const ServerIdType &id, std::vector<ChunkServerInfo> *chunkservers);

            /**
             * @brief Get a list of chunkservers on the server
             * @param ip server ip
             * @param[out] chunkservers A list of chunkserver information, valid when
             * the return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int ListChunkServersOnServer(
                const std::string &ip, std::vector<ChunkServerInfo> *chunkservers);

            /**
             * @brief Get detailed information about chunkserver
             * @param id chunkserver id
             * @param[out] chunkserver The detailed information of chunkserver is valid
             * when the return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int GetChunkServerInfo(const ChunkServerIdType &id,
                                           ChunkServerInfo *chunkserver);

            /**
             * @brief Get detailed information about chunkserver
             * @param csAddr The address of chunkserver, in the format of ip:port
             * @param[out] chunkserver The detailed information of chunkserver is valid
             * when the return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int GetChunkServerInfo(const std::string &csAddr,
                                           ChunkServerInfo *chunkserver);

            /**
             * @brief Get all copysets on chunkserver
             * @param id The id of chunkserver
             * @param[out] copysets Details of copysets on chunkserver, valid when the
             * return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int GetCopySetsInChunkServer(const ChunkServerIdType &id,
                                                 std::vector<CopysetInfo> *copysets);

            /**
             * @brief Get all copysets on chunkserver
             * @param csAddr The address of chunkserver, in the format of ip: port
             * @param[out] copysets Details of copysets on chunkserver, valid when the
             * return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int GetCopySetsInChunkServer(const std::string &csAddr,
                                                 std::vector<CopysetInfo> *copysets);

            /**
             * @brief Get all copysets in cluster
             * @param[out] the copyset list
             * @param[in] filterScaning whether need to filter copyset which in scaning
             * @return 0 if success, else return -1
             */
            virtual int GetCopySetsInCluster(std::vector<CopysetInfo> *copysetInfos,
                                             bool filterScaning = false);

            /**
             * @brief Get specify copyset
             * @param[in] lpid logical pool id
             * @param[in] copysetId copyset id
             * @param[out] copysetInfo the copyset
             * @return 0 if success, else return -1
             */
            virtual int GetCopyset(PoolIdType lpid, CopySetIdType copysetId,
                                   CopysetInfo *copysetInfo);

            /**
             * @brief List all servers in the cluster
             * @param[out] servers List of server information, valid when the return
             * value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int ListServersInCluster(std::vector<ServerInfo> *servers);

            /**
             * @brief List all chunkservers in the cluster
             * @param[out] chunkservers A list of server information, valid when the
             * return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int ListChunkServersInCluster(
                std::vector<ChunkServerInfo> *chunkservers);

            /**
             *  @brief list all the chunkservers with poolid in cluster
             *  @param[out] chunkservers chunkserver info
             *  @return succeed return 0; failed return -1;
             */
            virtual int ListChunkServersInCluster(
                std::map<PoolIdType, std::vector<ChunkServerInfo>> *chunkservers);

            /**
             *  @brief set copysets available flag
             *  @param copysets copysets going to be set available flag
             *  @param availFlag availble or not
             *  @return succeed return 0; failed return -1;
             */
            virtual int SetCopysetsAvailFlag(const std::vector<CopysetInfo> copysets,
                                             bool availFlag);

            /**
             *  @brief list all copysets that are unavailable
             *  @param[out] copysets copysets that are not availble currently
             *  @return succeed return 0; failed return -1;
             */
            virtual int ListUnAvailCopySets(std::vector<CopysetInfo> *copysets);

            /**
             * @brief Get the value of a metric for mds
             * @param metricName The name of the metric
             * @param[out] value The value of metric is valid when the return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int GetMetric(const std::string &metricName, uint64_t *value);

            /**
             * @brief Get the value of a metric for mds
             * @param metricName  The name of metric
             * @param[out] value The value of metric is valid when the return value is 0
             * @return returns 0 for success, -1 for failure
             */
            virtual int GetMetric(const std::string &metricName, std::string *value);

            /**
             * @brief sets userName and calls it when accessing the namespace interface
             * @param userName username
             */
            void SetUserName(const std::string &userName) { userName_ = userName; }

            /**
             * @brief sets the password and calls it when accessing the namespace
             * interface
             * @param password password
             */
            void SetPassword(const std::string &password) { password_ = password; }

            /**
             * @brief Get mds address list
             * @return List of mds addresses
             */
            virtual const std::vector<std::string> &GetMdsAddrVec() const
            {
                return mdsAddrVec_;
            }

            virtual const std::map<std::string, std::string> &GetDummyServerMap()
                const
            {
                return dummyServerMap_;
            }

            /**
             * @brief Get the address of the current mds
             */
            virtual std::vector<std::string> GetCurrentMds();

            /**
             * @brief sends rpc to mds to trigger fast leader balancing
             */
            virtual int RapidLeaderSchedule(PoolIdType lpid);

            /**
             * @brief Set specify logical pool to enable/disable scan
             * @param[in] lpid logical pool id
             * @param[in] scanEnable enable(true)/disable(false) scan
             * @return 0 if set success, else return -1
             */
            virtual int SetLogicalPoolScanState(PoolIdType lpid, bool scanEnable);

            /**
             * @brief to obtain mds online status,
             *          dummyserver is online and the dummyserver records a listen addr
             *          Only when the address is consistent with the mds address is
             * considered online
             * @param[out] onlineStatus mds online status, valid when returned to 0
             * @return returns 0 for success, -1 for failure
             */
            virtual void GetMdsOnlineStatus(std::map<std::string, bool> *onlineStatus);

            /**
             * @brief Get the recovery status of the specified chunkserver
             * @param[in] cs List of chunkservers to query
             * @param[out] statusMap returns the recovery status corresponding to each
             * chunkserver
             * @return returns 0 for success, -1 for failure
             */
            int QueryChunkServerRecoverStatus(
                const std::vector<ChunkServerIdType> &cs,
                std::map<ChunkServerIdType, bool> *statusMap);

            virtual int UpdateFileThrottleParams(
                const std::string &fileName, const curve::mds::ThrottleParams &params);

            int ListPoolset(std::vector<PoolsetInfo> *poolsets);

            int ListChunkFormatStatus(std::vector<ChunkFormatStatus> *formatStatuses);

        private:
            /**
             * @brief switch mds
             * @return returns true if the switch is successful, and false if all mds
             * fail
             */
            bool ChangeMDServer();

            /**
             * @brief sends RPC to mds for code reuse
             * @param
             * @return returns 0 for success, -1 for failure
             */
            template <typename T, typename Request, typename Response>
            int SendRpcToMds(Request *request, Response *response, T *obp,
                             void (T::*func)(google::protobuf::RpcController *,
                                             const Request *, Response *,
                                             google::protobuf::Closure *));

            /**
             * @brief Get a list of chunkservers on the server
             * @param request The request to be sent
             * @param[out] chunkservers A list of chunkserver information, valid when
             * the return value is 0
             * @return returns 0 for success, -1 for failure
             */
            int ListChunkServersOnServer(ListChunkServerRequest *request,
                                         std::vector<ChunkServerInfo> *chunkservers);

            /**
             * @brief Get detailed information about chunkserver
             * @param request The request to be sent
             * @param[out] chunkserver The detailed information of chunkserver is valid
             * when the return value is 0
             * @return returns 0 for success, -1 for failure
             */
            int GetChunkServerInfo(GetChunkServerInfoRequest *request,
                                   ChunkServerInfo *chunkserver);

            /**
             * @brief Get detailed information about chunkserver
             * @param request The request to be sent
             * @param[out] copysets Details of copysets on chunkserver, valid when the
             * return value is 0
             * @return returns 0 for success, -1 for failure
             */
            int GetCopySetsInChunkServer(GetCopySetsInChunkServerRequest *request,
                                         std::vector<CopysetInfo> *copysets);

            /**
             * @brief Initialize dummy server address
             * @param dummyPort dummy server port list
             * @return returns 0 for success, -1 for failure
             */
            int InitDummyServerMap(const std::string &dummyPort);

            /**
             * @brief: Obtain the listening address of mds through dummyServer
             * @param dummyAddr Address of dummyServer
             * @param[out] listenAddr mds listening address
             * @return returns 0 for success, -1 for failure
             */
            int GetListenAddrFromDummyPort(const std::string &dummyAddr,
                                           std::string *listenAddr);

            // Fill in the signature
            template <class T>
            void FillUserInfo(T *request);

            // client used to send HTTP requests
            MetricClient metricClient_;
            // Send RPC channel to mds
            brpc::Channel channel_;
            // Save vector for mds address
            std::vector<std::string> mdsAddrVec_;
            // Save the address of the dummy server corresponding to the mds address
            std::map<std::string, std::string> dummyServerMap_;
            // Save the current mds in mdsAddrVec_ Index in
            int currentMdsIndex_;
            // User name
            std::string userName_;
            // Password
            std::string password_;
            // Avoiding duplicate initialization
            bool isInited_;
        };
    } // namespace tool
} // namespace curve

#endif // SRC_TOOLS_MDS_CLIENT_H_

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
 * File Created: Monday, 17th September 2018 4:15:27 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_IOMANAGER4FILE_H_
#define SRC_CLIENT_IOMANAGER4FILE_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <atomic>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <string>
#include <memory>

#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/inflight_controller.h"
#include "src/client/iomanager.h"
#include "src/client/mds_client.h"
#include "src/client/metacache.h"
#include "src/client/request_scheduler.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "src/common/throttle.h"
#include "src/client/discard_task.h"

namespace curve {
namespace client {

using curve::common::Atomic;

class FlightIOGuard;

class IOManager4File : public IOManager {
 public:
    IOManager4File();
    ~IOManager4File() = default;

    /**
     *Initialization function
     * @param: filename is the file name of the current iomanager service
     * @param: ioopt is the configuration information of the current iomanager
     * @param: mdsclient penetrates downwards to Metacache
     * @return: Success true, failure false
     */
    bool Initialize(const std::string& filename,
                    const IOOption& ioOpt,
                    MDSClient* mdsclient);

    /**
     * @brief Recycle resources
     */
    void UnInitialize();

    /**
     *Synchronous mode reading
     * @param: buf is the current buffer to be read
     * @param: Cheap in offset file
     * @parma: length is the length to be read
     * @param: mdsclient transparently transmits to the underlying layer and communicates with mds when necessary
     * @return: Successfully returned reading the true length, -1 indicates failure
     */
    int Read(char* buf, off_t offset, size_t length, MDSClient* mdsclient);
    /**
     *Synchronous mode write
     * @param: mdsclient transparently transmits to the underlying layer and communicates with mds when necessary
     * @param: buf is the current buffer to be written
     * @param: Cheap in offset file
     * @param: length is the length to be read
     * @return: Success returns the true length of the write, -1 indicates failure
     */
    int Write(const char* buf, off_t offset, size_t length,
              MDSClient* mdsclient);
    /**
     *Asynchronous mode read
     * @param: mdsclient transparently transmits to the underlying layer and communicates with mds when necessary
     * @param: aioctx is an asynchronous read/write IO context that stores basic IO information
     * @param dataType type of aioctx->buf
     * @return: 0 indicates success, less than 0 indicates failure
     */
    int AioRead(CurveAioContext* aioctx, MDSClient* mdsclient,
                UserDataType dataType);
    /**
     *Asynchronous mode write
     * @param: mdsclient transparently transmits to the underlying layer and communicates with mds when necessary
     * @param: aioctx is an asynchronous read/write IO context that stores basic IO information
     * @param dataType type of aioctx->buf
     * @return: 0 indicates success, less than 0 indicates failure
     */
    int AioWrite(CurveAioContext* aioctx, MDSClient* mdsclient,
                 UserDataType dataType);

    /**
     * @brief Synchronous discard operation
     * @param offset discard offset
     * @param length discard length
     * @return On success, returns 0.
     *         On error, returns a negative value.
     */
    int Discard(off_t offset, size_t length, MDSClient* mdsclient);

    /**
     * @brief Asynchronous discard operation
     * @param aioctx async request context
     * @param mdsclient for communicate with MDS
     * @return 0 means success, otherwise it means failure
     */
    int AioDiscard(CurveAioContext* aioctx, MDSClient* mdsclient);

    /**
     * @brief Get rpc send token
     */
    void GetInflightRpcToken() override;

    /**
     * @brief Release RPC Send Token
     */
    void ReleaseInflightRpcToken() override;

    /**
     *Obtain Metacache, test code usage
     */
    MetaCache* GetMetaCache() {
        return &mc_;
    }
    /**
     *Set the scheduler to test the code using
     */
    void SetRequestScheduler(RequestScheduler* scheduler) {
        scheduler_ = scheduler;
    }

    /**
     *Obtain metric information and test code usage
     */
    FileMetric* GetMetric() {
        return fileMetric_;
    }

    /**
     *Reset IO configuration information for testing use
     */
    void SetIOOpt(const IOOption& opt) {
        ioopt_ = opt;
    }

    /**
     *Test usage, obtain request scheduler
     */
    RequestScheduler* GetScheduler() {
        return scheduler_;
    }

    /**
     *When the release executor detects a version update, it needs to notify the iomanager to update the file version information
     * @param: fi is the current file information that needs to be updated
     */
    void UpdateFileInfo(const FInfo_t& fi);

    const FInfo* GetFileInfo() const {
        return mc_.GetFileInfo();
    }

    void UpdateFileEpoch(const FileEpoch& fEpoch) {
        mc_.UpdateFileEpoch(fEpoch);
    }

    const FileEpoch* GetFileEpoch() const {
        return mc_.GetFileEpoch();
    }

    /**
     *Return the latest version number of the file
     */
    uint64_t GetLatestFileSn() const {
        return mc_.GetLatestFileSn();
    }

    /**
     *Update the latest version number of the file
     */
    void SetLatestFileSn(uint64_t newSn) {
        mc_.SetLatestFileSn(newSn);
    }

    /**
     * @brief get current file inodeid
     * @return file inodeid
     */
    uint64_t InodeId() const {
        return mc_.InodeId();
    }

    void UpdateFileThrottleParams(
        const common::ReadWriteThrottleParams& params);

    void SetDisableStripe();

 private:
    friend class LeaseExecutor;
    friend class FlightIOGuard;
    /**
     *Lease related interface, when LeaseExecutor contract renewal fails, calls LeaseTimeoutDisableIO
     *Failed to return all newly issued IOs
     */
    void LeaseTimeoutBlockIO();

    /**
     *When the lease is successfully renewed, the LeaseExecutor calls the interface to restore IO
     */
    void ResumeIO();

    /**
     *When the lesaexcitor detects a version change, it calls the interface and waits for inflight to return. During this period, IO is hanging
     */
    void BlockIO();

    /**
     *Because the bottom layer of the curve client is asynchronous IO, each IO is assigned an IOtracker to track IO
     *After this IO is completed, the underlying layer needs to inform the current IO manager to release this IOTracker,
     *HandleAsyncIOResponse is responsible for releasing the IOTracker
     * @param: iotracker is an asynchronous io returned
     */
    void HandleAsyncIOResponse(IOTracker* iotracker) override;

    class FlightIOGuard {
     public:
        explicit FlightIOGuard(IOManager4File* iomana) {
            iomanager = iomana;
            iomanager->inflightCntl_.IncremInflightNum();
        }

        ~FlightIOGuard() {
            iomanager->inflightCntl_.DecremInflightNum();
        }

     private:
        IOManager4File* iomanager;
    };

    bool IsNeedDiscard(size_t len) const;

 private:
    //Each IOManager has its IO configuration, which is saved in the iooption
    IOOption ioopt_;

    //Metacache stores all metadata information for the current file
    MetaCache mc_;

    //The IO is finally distributed by the schedule module to the chunkserver end, and the scheduler is created and released by the IOManager
    RequestScheduler* scheduler_;

    //Metric statistics on the client side
    FileMetric* fileMetric_;

    //The task thread pool is used to isolate the QEMU thread from the curve thread
    curve::common::TaskThreadPool<bthread::Mutex, bthread::ConditionVariable>
        taskPool_;

    //Inflight IO control
    InflightControl inflightCntl_;

    //Inflight rpc control
    InflightControl inflightRpcCntl_;

    std::unique_ptr<common::Throttle> throttle_;

    //Exit or not
    bool exit_;

    //The lease renewal thread and the QEMU side thread call are concurrent
    //QEMU will close the iomanager and its corresponding when calling close
    //Resources. The lease renewal thread will notify the iomanager when the renewal is successful or fails
    //The scheduler thread now requires block IO or resume IO, so
    //If the release renewal thread needs to notify the iomanager, then
    //If the resource scheduler of the iomanager has been released, it will
    //Causing a crash, so it is necessary to add a lock to this resource when exiting
    //There will be no concurrent situations, ensuring lease renewal when resources are deconstructed
    //Threads will no longer use these resources
    std::mutex exitMtx_;

    // enable/disable stripe for read/write of stripe file
    // currently only one scenario set this field to true:
    // chunkserver use client to read clone source file,
    // because client's IO requests already transformed by stripe parameters
    bool disableStripe_;

    std::unique_ptr<DiscardTaskManager> discardTaskManager_;
};

}  // namespace client
}  // namespace curve
#endif  // SRC_CLIENT_IOMANAGER4FILE_H_

/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Project: CurveCli
 * Created Date: 2022-05-11
 * Author: chengyi (Cyber-SiKu)
 */

package cmderror

import (
	"fmt"

	fscopyset "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/copyset"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/mds"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/copyset"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	bs_topo_statuscode "github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
)

// It is considered here that the importance of the error is related to the
// code, and the smaller the code, the more important the error is.
// Need to ensure that the smaller the code, the more important the error is
const (
	CODE_BASE_LINE   = 10000
	CODE_SUCCESS     = 0 * CODE_BASE_LINE
	CODE_RPC_RESULT  = 1 * CODE_BASE_LINE
	CODE_HTTP_RESULT = 2 * CODE_BASE_LINE
	CODE_RPC         = 3 * CODE_BASE_LINE
	CODE_HTTP        = 4 * CODE_BASE_LINE
	CODE_INTERNAL    = 9 * CODE_BASE_LINE
	CODE_UNKNOWN     = 10 * CODE_BASE_LINE
)

type CmdError struct {
	Code    int    `json:"code"`    // exit code
	Message string `json:"message"` // exit message
}

var (
	AllError []*CmdError
)

func init() {
	AllError = make([]*CmdError, 0)
}

func (ce *CmdError) ToError() error {
	if ce == nil {
		return nil
	}
	if ce.Code == CODE_SUCCESS {
		return nil
	}
	return fmt.Errorf(ce.Message)
}

func NewSucessCmdError() *CmdError {
	ret := &CmdError{
		Code:    CODE_SUCCESS,
		Message: "success",
	}
	AllError = append(AllError, ret)
	return ret
}

func NewInternalCmdError(code int, message string) *CmdError {
	if code == 0 {
		return NewSucessCmdError()
	}
	ret := &CmdError{
		Code:    CODE_INTERNAL + code,
		Message: message,
	}

	AllError = append(AllError, ret)
	return ret
}

func NewRpcError(code int, message string) *CmdError {
	if code == 0 {
		return NewSucessCmdError()
	}
	ret := &CmdError{
		Code:    CODE_RPC + code,
		Message: message,
	}
	AllError = append(AllError, ret)
	return ret
}

func NewRpcReultCmdError(code int, message string) *CmdError {
	if code == 0 {
		return NewSucessCmdError()
	}
	ret := &CmdError{
		Code:    CODE_RPC_RESULT + code,
		Message: message,
	}
	AllError = append(AllError, ret)
	return ret
}

func NewHttpError(code int, message string) *CmdError {
	if code == 0 {
		return NewSucessCmdError()
	}
	ret := &CmdError{
		Code:    CODE_HTTP + code,
		Message: message,
	}
	AllError = append(AllError, ret)
	return ret
}

func NewHttpResultCmdError(code int, message string) *CmdError {
	if code == 0 {
		return NewSucessCmdError()
	}
	ret := &CmdError{
		Code:    CODE_HTTP_RESULT + code,
		Message: message,
	}
	AllError = append(AllError, ret)
	return ret
}

func (cmd CmdError) TypeCode() int {
	return cmd.Code / CODE_BASE_LINE * CODE_BASE_LINE
}

func (cmd CmdError) TypeName() string {
	var ret string
	switch cmd.TypeCode() {
	case CODE_SUCCESS:
		ret = "success"
	case CODE_INTERNAL:
		ret = "internal"
	case CODE_RPC:
		ret = "rpc"
	case CODE_RPC_RESULT:
		ret = "rpcResult"
	case CODE_HTTP:
		ret = "http"
	case CODE_HTTP_RESULT:
		ret = "httpResult"
	default:
		ret = "unknown"
	}
	return ret
}

func (e *CmdError) Format(args ...interface{}) {
	e.Message = fmt.Sprintf(e.Message, args...)
}

// The importance of the error is considered to be related to the code,
// please use it under the condition that the smaller the code,
// the more important the error is.
func MostImportantCmdError(err []*CmdError) *CmdError {
	if len(err) == 0 {
		return NewSucessCmdError()
	}
	ret := err[0]
	for _, e := range err {
		if e.Code < ret.Code {
			ret = e
		}
	}
	return ret
}

// keep the most important wrong id, all wrong message will be kept
// if all success return success
func MergeCmdErrorExceptSuccess(err []*CmdError) *CmdError {
	if len(err) == 0 {
		return NewSucessCmdError()
	}
	var ret CmdError
	ret.Code = CODE_UNKNOWN
	ret.Message = ""
	countSuccess := 0
	for _, e := range err {
		if e != nil {
			if e.Code == CODE_SUCCESS {
				countSuccess++
				continue
			} else if e.Code < ret.Code {
				ret.Code = e.Code
			}
			ret.Message = e.Message + "\n" + ret.Message
		}
	}
	if countSuccess == len(err) {
		return NewSucessCmdError()
	}
	ret.Message = ret.Message[:len(ret.Message)-1]
	return &ret
}

// keep the most important wrong id, all wrong message will be kept
// if have one success return success
func MergeCmdError(err []*CmdError) *CmdError {
	if len(err) == 0 {
		return NewSucessCmdError()
	}
	var ret CmdError
	ret.Code = CODE_UNKNOWN
	ret.Message = ""
	for _, e := range err {
		if e.Code == CODE_SUCCESS {
			return e
		} else if e.Code < ret.Code {
			ret.Code = e.Code
		}
		ret.Message = e.Message + "\n" + ret.Message
	}
	ret.Message = ret.Message[:len(ret.Message)-1]
	return &ret
}

var (
	ErrSuccess = NewSucessCmdError
	Success    = ErrSuccess

	// internal error
	ErrHttpCreateGetRequest = func() *CmdError {
		return NewInternalCmdError(1, "create http get request failed, the error is: %s")
	}
	ErrDataNoExpected = func() *CmdError {
		return NewInternalCmdError(2, "data: %v is not as expected, the error is: %s")
	}
	ErrHttpClient = func() *CmdError {
		return NewInternalCmdError(3, "http client get error: %s")
	}
	ErrRpcDial = func() *CmdError {
		return NewInternalCmdError(4, "dial to rpc server %s failed, the error is: %s")
	}
	ErrUnmarshalJson = func() *CmdError {
		return NewInternalCmdError(5, "unmarshal json error, the json is %s, the error is %s")
	}
	ErrParseMetric = func() *CmdError {
		return NewInternalCmdError(6, "parse metric %s err!")
	}
	ErrGetMetaserverAddr = func() *CmdError {
		return NewInternalCmdError(7, "get metaserver addr failed, the error is: %s")
	}
	ErrGetClusterFsInfo = func() *CmdError {
		return NewInternalCmdError(8, "get cluster fs info failed, the error is: \n%s")
	}
	ErrGetAddr = func() *CmdError {
		return NewInternalCmdError(9, "invalid %s addr is: %s")
	}
	ErrMarShalProtoJson = func() *CmdError {
		return NewInternalCmdError(10, "marshal proto to json error, the error is: %s")
	}
	ErrUnknownFsType = func() *CmdError {
		return NewInternalCmdError(11, "unknown fs type: %s")
	}
	ErrAligned = func() *CmdError {
		return NewInternalCmdError(12, "%s should aligned with %s")
	}
	ErrUnknownBitmapLocation = func() *CmdError {
		return NewInternalCmdError(13, "unknown bitmap location: %s")
	}
	ErrParse = func() *CmdError {
		return NewInternalCmdError(14, "invalid %s: %s")
	}
	ErrSplitPeer = func() *CmdError {
		return NewInternalCmdError(15, "split peer[%s] failed, peer should be like: 127.0.0.1:8200:0")
	}
	ErrMarshalJson = func() *CmdError {
		return NewInternalCmdError(16, "marshal %s to json error, the error is: %s")
	}
	ErrCopysetKey = func() *CmdError {
		return NewInternalCmdError(17, "copyset key [%d] not found in %s")
	}
	ErrCopysetInfo = func() *CmdError {
		return NewInternalCmdError(17, "copyset[%d]: no leader peer!")
	}
	ErrQueryCopyset = func() *CmdError {
		return NewInternalCmdError(18, "query copyset failed! the error is: %s")
	}
	ErrOfflineCopysetPeer = func() *CmdError {
		return NewInternalCmdError(19, "peer [%s] is offline")
	}
	ErrStateCopysetPeer = func() *CmdError {
		return NewInternalCmdError(20, "state in peer[%s]: %s")
	}
	ErrListCopyset = func() *CmdError {
		return NewInternalCmdError(21, "list copyset failed! the error is: %s")
	}
	ErrCheckCopyset = func() *CmdError {
		return NewInternalCmdError(22, "check copyset failed! the error is: %s")
	}
	ErrEtcdOffline = func() *CmdError {
		return NewInternalCmdError(23, "etcd[%s] is offline")
	}
	ErrMdsOffline = func() *CmdError {
		return NewInternalCmdError(24, "mds[%s] is offline")
	}
	ErrMetaserverOffline = func() *CmdError {
		return NewInternalCmdError(25, "metaserver[%s] is offline")
	}
	ErrCheckPoolTopology = func() *CmdError {
		return NewInternalCmdError(26, "pool[%s] is not in cluster nor in json file")
	}
	ErrReadFile = func() *CmdError {
		return NewInternalCmdError(27, "read file[%s] failed! the error is: %s")
	}
	ErrGetFsPartition = func() *CmdError {
		return NewInternalCmdError(28, "get fs partition failed! the error is: %s")
	}
	ErrTopology = func() *CmdError {
		return NewInternalCmdError(29, "%s[%d] belongs to %s[%d] who was not found")
	}
	ErrCopysetGapKey = func() *CmdError {
		return NewInternalCmdError(30, "fail to parse copyset key! the line is: %s")
	}
	ErrCopysetGapState = func() *CmdError {
		return NewInternalCmdError(30, "fail to parse copyset[%d] state! the line is: %s")
	}
	ErrCopysetGapLastLogId = func() *CmdError {
		return NewInternalCmdError(31, "fail to parse copyset[%d] last_log_id! the line is: %s")
	}
	ErrCopysetGapReplicator = func() *CmdError {
		return NewInternalCmdError(32, "fail to parse copyset[%d] replicator! the line is: %s")
	}
	ErrCopysetGap = func() *CmdError {
		return NewInternalCmdError(33, "fail to parse copyset[%d]: state or storage or replicator is not found!")
	}
	ErrSplitMountpoint = func() *CmdError {
		return NewInternalCmdError(30, "invalid mountpoint[%s], should be like: hostname:port:path")
	}
	ErrGetMountpoint = func() *CmdError {
		return NewInternalCmdError(31, "get mountpoint failed! the error is: %s")
	}
	ErrWriteFile = func() *CmdError {
		return NewInternalCmdError(32, "write file[%s] failed! the error is: %s")
	}
	ErrSetxattr = func() *CmdError {
		return NewInternalCmdError(33, "setxattr [%s] failed! the error is: %s")
	}
	ErrBsGetPhysicalPool = func() *CmdError {
		return NewInternalCmdError(34, "list physical pool fail, the error is: %s")
	}
	ErrBsGetAllocatedSize = func() *CmdError {
		return NewInternalCmdError(35, "get file allocated fail, the error is: %s")
	}
	ErrGettimeofday = func() *CmdError {
		return NewInternalCmdError(36, "get time of day fail, the error is: %s")
	}
	ErrBsGetFileInfo = func() *CmdError {
		return NewInternalCmdError(37, "get file info fail, the error is: %s")
	}
	ErrBsGetFileSize = func() *CmdError {
		return NewInternalCmdError(38, "get file size fail, the error is: %s")
	}
	ErrBsListZone = func() *CmdError {
		return NewInternalCmdError(39, "list zone fail. the error is %s")
	}

	ErrBsDeleteFile = func() *CmdError {
		return NewInternalCmdError(40, "delete file fail. the error is %s")
	}

	ErrRespTypeNoExpected = func() *CmdError {
		return NewInternalCmdError(41, "the response type is not as expected, should be: %s")
	}

	ErrGetPeer = func() *CmdError {
		return NewInternalCmdError(42, "invalid peer args, err: %s")
	}
	ErrQueryWarmup = func() *CmdError {
		return NewInternalCmdError(43, "query warmup progress fail, err: %s")
	}
	ErrBsGetSegment = func() *CmdError {
		return NewInternalCmdError(44, "get segments fail, err: %s")
	}
	ErrBsGetChunkCopyset = func() *CmdError {
		return NewInternalCmdError(45, "get copyset of chunk fail, err: %s")
	}
	ErrBsChunkServerListInCopySets = func() *CmdError {
		return NewInternalCmdError(46, "get chunkserver list in copysets fail, err: %s")
	}
	ErrBsUnknownFileType = func() *CmdError {
		return NewInternalCmdError(47, "unknown file type[%s], only support: dir, file")
	}
	ErrBsCreateFileOrDirectoryType = func() *CmdError {
		return NewInternalCmdError(48, "create file or directory fail, err: %s")
	}
	ErrBsListLogicalPoolInfo = func() *CmdError {
		return NewInternalCmdError(49, "list logical pool info fail, the error is: %s")
	}
	ErrBsUnknownThrottleType = func() *CmdError {
		return NewInternalCmdError(50, "unknown throttle type[%s], only support: iops_total|iops_read|iops_write|bps_total|bps_read|bps_write")
	}
	ErrBsListDir = func() *CmdError {
		return NewInternalCmdError(51, "list directory fail, err: %s")
	}
	ErrBsGetCopysetStatus = func() *CmdError {
		return NewInternalCmdError(52, "get copyset status fail, err: %s")
	}
	ErrBsOpNameNotSupport = func() *CmdError {
		return NewInternalCmdError(53, "not support op[%s], only support: operator, change_peer, add_peer, remove_peer, transfer_leader")
	}
	ErrBsGetClientList = func() *CmdError {
		return NewInternalCmdError(54, "get client list fail, err: %s")
	}
	ErrBsGetClientStatus = func() *CmdError {
		return NewInternalCmdError(55, "get client status fail, err: %s")
	}
	ErrBsGetEtcdStatus = func() *CmdError {
		return NewInternalCmdError(56, "get etcd status fail, err: %s")
	}
	ErrBsGetMdsStatus = func() *CmdError {
		return NewInternalCmdError(57, "get mds status fail, err: %s")
	}
	ErrBsGetSnapshotServerStatus = func() *CmdError {
		return NewInternalCmdError(58, "get snapshotserver status fail, err: %s")
	}

	// http error
	ErrHttpUnreadableResult = func() *CmdError {
		return NewHttpResultCmdError(1, "http response is unreadable, the uri is: %s, the error is: %s")
	}
	ErrHttpResultNoExpected = func() *CmdError {
		return NewHttpResultCmdError(2, "http response is not expected, the hosts is: %s, the suburi is: %s, the result is: %s")
	}
	ErrHttpStatus = func(statusCode int) *CmdError {
		return NewHttpError(statusCode, "the url is: %s, http status code is: %d")
	}

	// rpc error
	ErrRpcCall = func() *CmdError {
		return NewRpcReultCmdError(1, "rpc[%s] is fail, the error is: %s")
	}
	ErrUmountFs = func(statusCode int) *CmdError {
		var message string
		code := mds.FSStatusCode(statusCode)
		switch code {
		case mds.FSStatusCode_OK:
			message = "success"
		case mds.FSStatusCode_MOUNT_POINT_NOT_EXIST:
			message = "mountpoint not exist"
		case mds.FSStatusCode_NOT_FOUND:
			message = "fs not found"
		case mds.FSStatusCode_FS_BUSY:
			message = "mountpoint is busy"
		default:
			message = fmt.Sprintf("umount from fs failed!, error is %s", code.String())
		}
		return NewRpcReultCmdError(statusCode, message)
	}
	ErrGetFsInfo = func(statusCode int) *CmdError {
		return NewRpcReultCmdError(statusCode, "get fs info failed: status code is %s")
	}
	ErrGetMetaserverInfo = func(statusCode int) *CmdError {
		return NewRpcReultCmdError(statusCode, "get metaserver info failed: status code is %s")
	}
	ErrGetCopysetOfPartition = func(statusCode int) *CmdError {
		code := topology.TopoStatusCode(statusCode)
		message := fmt.Sprintf("get copyset of partition failed: status code is %s", code.String())
		return NewRpcReultCmdError(statusCode, message)
	}
	ErrDeleteFs = func(statusCode int) *CmdError {
		var message string
		code := mds.FSStatusCode(statusCode)
		switch code {
		case mds.FSStatusCode_OK:
			message = "success"
		case mds.FSStatusCode_NOT_FOUND:
			message = "fs not found!"
		default:
			message = fmt.Sprintf("delete fs failed!, error is %s", code.String())
		}
		return NewRpcReultCmdError(statusCode, message)
	}
	ErrCreateFs = func(statusCode int) *CmdError {
		var message string
		code := mds.FSStatusCode(statusCode)
		switch code {
		case mds.FSStatusCode_OK:
			message = "success"
		case mds.FSStatusCode_FS_EXIST:
			message = "fs exist, but s3 info is not inconsistent"
		case mds.FSStatusCode_S3_INFO_ERROR:
			message = "s3 info is not available"
		case mds.FSStatusCode_FSNAME_INVALID:
			message = "fsname should match regex: ^([a-z0-9]+\\-?)+$"
		default:
			message = fmt.Sprintf("delete fs failed!, error is %s", mds.FSStatusCode_name[int32(code)])
		}
		return NewRpcReultCmdError(statusCode, message)
	}
	ErrGetCopysetsInfo = func(statusCode int) *CmdError {
		code := topology.TopoStatusCode(statusCode)
		message := fmt.Sprintf("get copysets info failed: status code is %s", code.String())
		return NewRpcReultCmdError(statusCode, message)
	}
	ErrBsCopysetOpStatus = func(statusCode copyset.COPYSET_OP_STATUS, addr string) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case copyset.COPYSET_OP_STATUS_COPYSET_OP_STATUS_COPYSET_NOTEXIST:
			message = fmt.Sprintf("not exist in %s", addr)
		case copyset.COPYSET_OP_STATUS_COPYSET_OP_STATUS_SUCCESS:
			message = "ok"
		default:
			message = fmt.Sprintf("op status: %s in %s", statusCode.String(), addr)
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrListPool = func(statusCode topology.TopoStatusCode) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case topology.TopoStatusCode_TOPO_OK:
			message = "ok"
		default:
			message = fmt.Sprintf("list topology err: %s", statusCode.String())
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrListZone = func(statusCode topology.TopoStatusCode) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case topology.TopoStatusCode_TOPO_OK:
			message = "ok"
		default:
			message = fmt.Sprintf("list Zone err: %s", statusCode.String())
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrListServer = func(statusCode topology.TopoStatusCode) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case topology.TopoStatusCode_TOPO_OK:
			message = "ok"
		default:
			message = fmt.Sprintf("list Server fail, err: %s", statusCode.String())
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrDeleteTopology = func(statusCode topology.TopoStatusCode, topoType string, name string) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case topology.TopoStatusCode_TOPO_OK:
			message = "ok"
		default:
			message = fmt.Sprintf("delete %s[%s], err: %s", topoType, name, statusCode.String())
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrCreateTopology = func(statusCode topology.TopoStatusCode, topoType string, name string) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case topology.TopoStatusCode_TOPO_OK:
			message = "ok"
		default:
			message = fmt.Sprintf("create %s[%s], err: %s", topoType, name, statusCode.String())
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrCopysetOpStatus = func(statusCode fscopyset.COPYSET_OP_STATUS, addr string) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case fscopyset.COPYSET_OP_STATUS_COPYSET_OP_STATUS_COPYSET_NOTEXIST:
			message = fmt.Sprintf("not exist in %s", addr)
		case fscopyset.COPYSET_OP_STATUS_COPYSET_OP_STATUS_SUCCESS:
			message = "ok"
		default:
			message = fmt.Sprintf("op status: %s in %s", statusCode.String(), addr)
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrCreateCacheClusterRpc = func(statusCode topology.TopoStatusCode) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case topology.TopoStatusCode_TOPO_OK:
			message = "success"
		case topology.TopoStatusCode_TOPO_INVALID_PARAM:
			message = "no server in request"
		case topology.TopoStatusCode_TOPO_IP_PORT_DUPLICATED:
			message = "some servers are already in other cluster"
		case topology.TopoStatusCode_TOPO_ALLOCATE_ID_FAIL:
			message = "allocate cluster id failed"
		case topology.TopoStatusCode_TOPO_STORGE_FAIL:
			message = "storage cluster info to etcd(or other thing failed)"
		default:
			message = "unknown error"
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrListMemcacheCluster = func(statusCode topology.TopoStatusCode) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case topology.TopoStatusCode_TOPO_OK:
			message = "success"
		case topology.TopoStatusCode_TOPO_MEMCACHECLUSTER_NOT_FOUND:
			message = "no memcacheCluster in the curvefs"
		default:
			message = "unknown error"
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrBsListPhysicalPoolRpc = func(statusCode bs_topo_statuscode.TopoStatusCode) *CmdError {
		code := int32(statusCode)
		message := fmt.Sprintf("Rpc[ListPhysicalPool] status code: %s", bs_topo_statuscode.TopoStatusCode_name[code])
		return NewRpcReultCmdError(int(-code), message)
	}
	ErrBsGetAllocatedSizeRpc = func(statuscode nameserver2.StatusCode, file string) *CmdError {
		message := fmt.Sprintf("Rpc[GetFileAllocatedSize] for [%s] status code: %s", file, statuscode.String())
		return NewInternalCmdError(int(statuscode), message)
	}
	ErrBsGetFileInfoRpc = func(statuscode nameserver2.StatusCode, file string) *CmdError {
		message := fmt.Sprintf("Rpc[GetFileInfo] for [%s] status code: %s", file, statuscode.String())
		return NewInternalCmdError(int(statuscode), message)
	}
	ErrBsGetFileSizeRpc = func(statuscode nameserver2.StatusCode, file string) *CmdError {
		message := fmt.Sprintf("Rpc[GetFileSize] for [%s] status code: %s", file, statuscode.String())
		return NewInternalCmdError(int(statuscode), message)
	}
	ErrBsListPoolZoneRpc = func(statuscode bs_topo_statuscode.TopoStatusCode) *CmdError {
		message := fmt.Sprintf("Rpc[ListPoolZone] faild status code: %s", statuscode.String())
		return NewInternalCmdError(int(statuscode), message)
	}

	// bs
	ErrCreateBsTopology = func(statusCode bs_topo_statuscode.TopoStatusCode, topoType string, name string) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case bs_topo_statuscode.TopoStatusCode_Success:
			message = "ok"
		default:
			message = fmt.Sprintf("create %s[%s], err: %s", topoType, name, statusCode.String())
		}
		return NewRpcReultCmdError(-code, message)
	}
	ErrListZonesInPhyPool = func(statusCode bs_topo_statuscode.TopoStatusCode) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case bs_topo_statuscode.TopoStatusCode_Success:
			message = "list zones successfully"
		default:
			message = fmt.Sprintf("list zones in physicalpool err: %s", statusCode.String())
		}
		return NewRpcReultCmdError(-code, message)
	}
	ErrListServers = func(statusCode bs_topo_statuscode.TopoStatusCode) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case bs_topo_statuscode.TopoStatusCode_Success:
			message = "list servers successfully"
		default:
			message = fmt.Sprintf("list servers err: %s", statusCode.String())
		}
		return NewRpcReultCmdError(-code, message)
	}

	ErrListZones = func(statusCode bs_topo_statuscode.TopoStatusCode) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case bs_topo_statuscode.TopoStatusCode_Success:
			message = "ok"
		default:
			message = fmt.Sprintf("list topology err: %s", statusCode.String())
		}
		return NewRpcReultCmdError(-code, message)
	}

	ErrDelServer = func(statusCode bs_topo_statuscode.TopoStatusCode, topoType string, name string) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case bs_topo_statuscode.TopoStatusCode_Success:
			message = "delete servers successfully"
		default:
			message = fmt.Sprintf("delete %s[%s], err: %s", topoType, name, statusCode.String())
		}
		return NewRpcReultCmdError(-code, message)
	}

	ErrDelZone = func(statusCode bs_topo_statuscode.TopoStatusCode, topoType string, name string) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case bs_topo_statuscode.TopoStatusCode_Success:
			message = "list zones successfully"
		default:
			message = fmt.Sprintf("delete %s[%s], err: %s", topoType, name, statusCode.String())
		}
		return NewRpcReultCmdError(-code, message)
	}

	ErrDelPhyPool = func(statusCode bs_topo_statuscode.TopoStatusCode, topoType string, name string) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case bs_topo_statuscode.TopoStatusCode_Success:
			message = "list physicalpools successfully"
		default:
			message = fmt.Sprintf("delete %s[%s], err: %s", topoType, name, statusCode.String())
		}
		return NewRpcReultCmdError(-code, message)
	}

	ErrGetOrAllocateSegment = func(statusCode nameserver2.StatusCode, file string, offset uint64) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case nameserver2.StatusCode_kOK:
			message = "getOrAllocateSegment  successfully"
		default:
			message = fmt.Sprintf("getOrAllocateSegment file[%s] offset[%d], err: %s", file, offset, statusCode.String())
		}
		return NewRpcReultCmdError(code, message)
	}

	ErrGetChunkServerListInCopySets = func(statusCode statuscode.TopoStatusCode, logicalPool uint32, copysetIds []uint32) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case statuscode.TopoStatusCode_Success:
			message = "getChunkServerListInCopySets successfully"
		default:
			message = fmt.Sprintf("getChunkServerListInCopySets logicalPool[%d] copysets%v, err: %s", logicalPool, copysetIds, statusCode.String())
		}
		return NewRpcReultCmdError(-code, message)
	}

	ErrExtendFile = func(statusCode nameserver2.StatusCode, path, size string) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case nameserver2.StatusCode_kOK:
			message = "successfully expanded the file"
		default:
			message = fmt.Sprintf("failed to expand file[%s] to %s, err: %s", path, size, statusCode.String())
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrCreateFile = func(statusCode nameserver2.StatusCode, path string) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case nameserver2.StatusCode_kOK:
			message = "Created successfully"
		default:
			message = fmt.Sprintf("failed to create file[%s], err: %s", path, statusCode.String())
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrUpdateFileThrottle = func(statusCode nameserver2.StatusCode, path string) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case nameserver2.StatusCode_kOK:
			message = "successfully update the file throttle"
		default:
			message = fmt.Sprintf("failed to update file[%s] throttle, err: %s", path, statusCode.String())
		}
		return NewRpcReultCmdError(code, message)
	}
	ErrBsGetCopyset = func(statusCode statuscode.TopoStatusCode, logicalpoolid, copysetid uint32) *CmdError {
		var message string
		code := int(statusCode)
		switch statusCode {
		case statuscode.TopoStatusCode_Success:
			message = "success"
		default:
			message = fmt.Sprintf("get copyset(id: %d,logicalPoolid: %d) info fail, err: %s", copysetid, logicalpoolid, statusCode.String())
		}
		return NewRpcReultCmdError(code, message)
	}
)

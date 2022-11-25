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
 * Created Date: 2022-11-04
 * Author: ShiYue
 */

package consistency

import (
	"fmt"
	"strconv"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/query/file"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
)

const (
	Exam = "curve bs check consistency --path=/file1 --hashcheck=false/true"
)

type ConsistencyCmd struct {
	basecmd.FinalCurveCmd
	timeout         time.Duration
	retryTimes      int32
	addrs           []string
	filename        string
	port            string
	hash_check      bool
	isInConsistency bool
	applyIndex      bool

	hashErr []*cmderror.CmdError

	//Segment
	getOrAllcSegInfoRpc *GetOrAllcSegInfoRpc
	pageFileSegs        []*nameserver2.PageFileSegment

	//copyset
	getCksInCopySetsRpc *GetChunkServerListInCopySetsRpc
	lpid2cpIds          *Set
	cpId2lpId           map[uint32]uint32
	csAddr2Copyset      map[string]uint32
	copyset2csAddrs     map[uint32][]string
	chunksInCopyset     map[uint32][]uint64 //一个copyset里的多个chunkid

	//copysetStatus
	getCopysetStatusRpc *GetCopysetStatusRpc

	//chunkHash
	getChunkHashRpc *GetChunkHashRpc

	errIdxRows  []map[string]string
	errHashRows []map[string]string
}

func NewConsistencyCmd() *cobra.Command {
	csCmd := &ConsistencyCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "consistency",
			Short:   "check file consistency in curve bs",
			Example: Exam,
		},
	}
	basecmd.NewFinalCurveCli(&csCmd.FinalCurveCmd, csCmd)
	return csCmd.Cmd
}

var _ basecmd.FinalCurveCmdFunc = (*ConsistencyCmd)(nil) // check interface

func (csCmd *ConsistencyCmd) AddFlags() {
	config.AddRpcRetryTimesFlag(csCmd.Cmd)
	config.AddRpcTimeoutFlag(csCmd.Cmd)
	config.AddBsMdsFlagOption(csCmd.Cmd)
	config.AddBsPathRequiredFlag(csCmd.Cmd)
	config.AddBsHashCheckRequiredFlag(csCmd.Cmd)
	config.AddBsUserOptionFlag(csCmd.Cmd)
	config.AddBsPasswordOptionFlag(csCmd.Cmd)
	config.AddBsPortOptionFlag(csCmd.Cmd)
}

func InitBaseRpcCall(csCmd *ConsistencyCmd) error {
	addrs, addrErr := config.GetBsMdsAddrSlice(csCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	csCmd.addrs = addrs
	csCmd.timeout = config.GetBsFlagDuration(csCmd.Cmd, config.RPCTIMEOUT)
	csCmd.retryTimes = config.GetBsFlagInt32(csCmd.Cmd, config.RPCRETRYTIMES)
	csCmd.filename = config.GetBsFlagString(csCmd.Cmd, config.CURVEBS_PATH)
	csCmd.hash_check = config.GetBsFlagBool(csCmd.Cmd, config.CURVEBS_HASH_CHECK)
	csCmd.port = config.GetBsFlagString(csCmd.Cmd, config.CURVEBS_PORT)
	return nil
}

// generate mappings of logicalpool id to copyset ids、copyset id to logicalpoolid and copyset id to chunk ids
func (csCmd *ConsistencyCmd) GenCopysetMappings(filename string) (*Set, map[uint32]uint32) {
	lpID2CpsIds := NewSet()              // mapping of logicalpool id to copyset id
	cpId2LpId := make(map[uint32]uint32) // mapping of copyset id to logicalpool id
	for _, seg := range csCmd.pageFileSegs {
		lpid := seg.GetLogicalPoolID()
		var cpIds []uint32
		for _, ck := range seg.GetChunks() {
			copysetId := ck.GetCopysetID()
			// mapping of copyset id to chunk ids
			csCmd.chunksInCopyset[copysetId] = append(csCmd.chunksInCopyset[copysetId], ck.GetChunkID())

			// copyset去重, 因为多个chunkserver的copysetId值可能相同,避免重复查询copyset
			index := slices.Index(cpIds, copysetId)
			if index == -1 {
				cpIds = append(cpIds, copysetId)
				lpID2CpsIds.Exsit(lpid, copysetId)
				cpId2LpId[copysetId] = lpid
			}
		}
	}
	return lpID2CpsIds, cpId2LpId
}

func (csCmd *ConsistencyCmd) Init(cmd *cobra.Command, args []string) error {
	err := InitBaseRpcCall(csCmd)
	if err != nil {
		return err
	}
	csCmd.chunksInCopyset = make(map[uint32][]uint64)
	csCmd.cpId2lpId = make(map[uint32]uint32)
	csCmd.csAddr2Copyset = make(map[string]uint32)
	csCmd.copyset2csAddrs = make(map[uint32][]string)
	csCmd.isInConsistency = false
	csCmd.applyIndex = true
	csCmd.lpid2cpIds = NewSet()

	res, err1 := file.GetFileInfo(csCmd.Cmd)
	if err1.TypeCode() != cmderror.CODE_SUCCESS {
		retErr := cmderror.ErrBsGetFileInfo()
		retErr.Format(err1.Message)
		return retErr.ToError()
	}
	//csCmd.fileInfo = res.GetFileInfo()
	pfSegs, err1 := csCmd.GetFileSegments(csCmd.filename, res.GetFileInfo())
	if err1.TypeCode() != cmderror.CODE_SUCCESS {
		return err1.ToError()
	}
	csCmd.pageFileSegs = pfSegs
	csCmd.lpid2cpIds, csCmd.cpId2lpId = csCmd.GenCopysetMappings(csCmd.filename)
	header := []string{"host", "chunkserver", "copysetId", "groupId", "logicalpoolId", "chunkId"}
	csCmd.SetHeader(header)
	csCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		csCmd.Header, []string{"host", "chunkserver", "copysetId", "groupId"},
	))
	return err1.ToError()
}

func (csCmd *ConsistencyCmd) CheckCopysetConsistency(lpid2cpIds *Set) *cmderror.CmdError {
	for lpid, cpsIds := range lpid2cpIds.Elems {
		res, err := csCmd.GetChunkServerListOfCopySets(lpid, cpsIds)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		copysetServersInfos := res.GetCsInfo()
		for _, serverInfo := range copysetServersInfos {
			copysetId := serverInfo.GetCopysetId()
			var csAddrs []string
			for _, csLoc := range serverInfo.GetCsLocs() {
				csAddr := csLoc.GetHostIp() + ":" + strconv.FormatUint(uint64(csLoc.GetPort()), 10)
				csAddrs = append(csAddrs, csAddr)
				csCmd.csAddr2Copyset[csAddr] = copysetId
			}
			csCmd.copyset2csAddrs[copysetId] = csAddrs
		}
	}
	if csCmd.hash_check {
		for cpid, csAddrs := range csCmd.copyset2csAddrs {
			err1 := csCmd.CheckApplyIndex(cpid, csAddrs)
			if err1.TypeCode() != cmderror.CODE_SUCCESS {
				return err1
			}
		}
		fmt.Println("Apply indexs are consistent")
		hashConsistency := true
		for cpid, csAddrs := range csCmd.copyset2csAddrs {
			err2, hash := csCmd.CheckCopysetHash(cpid, csAddrs)
			if err2.TypeCode() != cmderror.CODE_SUCCESS && hash {
				return err2
			}
			if err2.TypeCode() != cmderror.CODE_SUCCESS && !hash {
				hashConsistency = hash
				fmt.Println("chunk hashs are not consistent!")
			}
		}
		if hashConsistency {
			fmt.Println("chunk hashs are consistent")
		}

		return cmderror.ErrSuccess()
	} else {
		for cpid, csAddrs := range csCmd.copyset2csAddrs {
			err := csCmd.CheckApplyIndex(cpid, csAddrs)
			if err.TypeCode() != cmderror.CODE_SUCCESS {
				return err
			}
		}
	}
	fmt.Println("Apply indexs are consistent")
	return cmderror.ErrSuccess()
}

func (csCmd *ConsistencyCmd) RunCommand(cmd *cobra.Command, args []string) error {
	err := csCmd.CheckCopysetConsistency(csCmd.lpid2cpIds)
	return err.ToError()
}

func (csCmd *ConsistencyCmd) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&csCmd.FinalCurveCmd, csCmd)
}

func (csCmd *ConsistencyCmd) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&csCmd.FinalCurveCmd)
}

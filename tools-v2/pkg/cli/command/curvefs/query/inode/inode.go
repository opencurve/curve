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
 * Created Date: 2022-06-28
 * Author: chengyi (Cyber-SiKu)
 */

package inode

import (
	"context"
	"fmt"

	"github.com/liushuochen/gotable"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/list/partition"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs/query/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/metaserver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

const (
	inodeExample = `$ curve fs query inode --fsid 1 --inodeid 1`
)

const (
	ROW_FS_ID               = "fs id"
	ROW_INODE_ID            = "inode id"
	ROW_LENGTH              = "length"
	ROW_TYPE                = "type"
	ROW_NLINK               = "nlink"
	ROW_PARENT              = "parent"
	ROW_S3CHUNKINFO_CHUNKID = "s3 chunk id"
	ROW_S3CHUNKINFO_OFFSET  = "s3 offset"
	ROW_S3CHUNKINFO_LENGTH  = "s3 length"
	ROW_S3CHUNKINFO_SIZE    = "s3 size"
)

type QueryInodeRpc struct {
	Info             *basecmd.Rpc
	Request          *metaserver.GetInodeRequest
	metaserverClient metaserver.MetaServerServiceClient
}

var _ basecmd.RpcFunc = (*QueryInodeRpc)(nil) // check interface

func (qiRpc *QueryInodeRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	qiRpc.metaserverClient = metaserver.NewMetaServerServiceClient(cc)
}

func (qiRpc *QueryInodeRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return qiRpc.metaserverClient.GetInode(ctx, qiRpc.Request)
}

type InodeCommand struct {
	basecmd.FinalCurveCmd
	QIRpc *QueryInodeRpc
}

var _ basecmd.FinalCurveCmdFunc = (*InodeCommand)(nil) // check interface

func NewInodeCommand() *cobra.Command {
	inodeCmd := &InodeCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "inode",
			Short:   "query the inode of fs",
			Example: inodeExample,
		},
	}
	basecmd.NewFinalCurveCli(&inodeCmd.FinalCurveCmd, inodeCmd)
	return inodeCmd.Cmd
}

func (iCmd *InodeCommand) AddFlags() {
	config.AddRpcRetryTimesFlag(iCmd.Cmd)
	config.AddRpcTimeoutFlag(iCmd.Cmd)
	config.AddFsMdsAddrFlag(iCmd.Cmd)
	config.AddFsIdRequiredFlag(iCmd.Cmd)
	config.AddInodeIdRequiredFlag(iCmd.Cmd)
}

func (iCmd *InodeCommand) Init(cmd *cobra.Command, args []string) error {
	table, err := gotable.Create(ROW_FS_ID, ROW_INODE_ID, ROW_LENGTH, ROW_TYPE, ROW_NLINK, ROW_PARENT)
	if err != nil {
		return err
	}
	iCmd.Table = table

	return nil
}

func (iCmd *InodeCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&iCmd.FinalCurveCmd, iCmd)
}

func (iCmd *InodeCommand) Prepare() error {
	fsId := config.GetFlagUint32(iCmd.Cmd, config.CURVEFS_FSID)
	inodeId := config.GetFlagUint64(iCmd.Cmd, config.CURVEFS_INODEID)

	fsId2PartitionList, errGet := partition.GetFsPartition(iCmd.Cmd)
	if errGet.TypeCode() != cmderror.CODE_SUCCESS {
		return errGet.ToError()
	}
	partitionInfoList := (*fsId2PartitionList)[fsId]
	if partitionInfoList == nil {
		return fmt.Errorf("inode[%d] is not found in fs[%d]", inodeId, fsId)
	}
	index := slices.IndexFunc(partitionInfoList,
		func(p *common.PartitionInfo) bool {
			return p.GetFsId() == fsId && p.GetStart() <= inodeId && p.GetEnd() >= inodeId
		})
	if index < 0 {
		return fmt.Errorf("inode[%d] is not on any partition of fs[%d]", inodeId, fsId)
	}
	partitionInfo := partitionInfoList[index]
	poolId := partitionInfo.GetPoolId()
	copyetId := partitionInfo.GetCopysetId()
	partitionId := partitionInfo.GetPartitionId()
	supportStream := false
	inodeRequest := &metaserver.GetInodeRequest{
		PoolId:           &poolId,
		CopysetId:        &copyetId,
		PartitionId:      &partitionId,
		FsId:             &fsId,
		InodeId:          &inodeId,
		SupportStreaming: &supportStream,
	}
	iCmd.QIRpc = &QueryInodeRpc{
		Request: inodeRequest,
	}
	// get addrs
	config.AddCopysetidSliceRequiredFlag(iCmd.Cmd)
	config.AddPoolidSliceRequiredFlag(iCmd.Cmd)
	iCmd.Cmd.ParseFlags([]string{
		fmt.Sprintf("--%s", config.CURVEFS_COPYSETID), fmt.Sprintf("%d", copyetId),
		fmt.Sprintf("--%s", config.CURVEFS_POOLID), fmt.Sprintf("%d", poolId),
	})
	key2Copyset, errQuery := copyset.QueryCopysetInfo(iCmd.Cmd)
	if errQuery.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf("query copyset info failed: %s", errQuery.Message)
	}
	if len(*key2Copyset) == 0 {
		return fmt.Errorf("no copysetinfo found")
	}
	key := cobrautil.GetCopysetKey(uint64(poolId), uint64(copyetId))
	leader := (*key2Copyset)[key].Info.GetLeaderPeer()
	addr, peerErr := cobrautil.PeertoAddr(leader)
	if peerErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf("pares leader peer[%s] failed: %s", leader, peerErr.Message)
	}
	addrs := []string{addr}

	timeout := viper.GetDuration(config.VIPER_GLOBALE_RPCTIMEOUT)
	retrytimes := viper.GetInt32(config.VIPER_GLOBALE_RPCRETRYTIMES)
	iCmd.QIRpc.Info = basecmd.NewRpc(addrs, timeout, retrytimes, "GetInode")
	return nil
}

func (iCmd *InodeCommand) RunCommand(cmd *cobra.Command, args []string) error {
	preErr := iCmd.Prepare()
	if preErr != nil {
		return preErr
	}

	inodeResult, err := basecmd.GetRpcResponse(iCmd.QIRpc.Info, iCmd.QIRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf("get inode failed: %s", err.Message)
	}
	getInodeResponse := inodeResult.(*metaserver.GetInodeResponse)
	if getInodeResponse.GetStatusCode() != metaserver.MetaStatusCode_OK {
		return fmt.Errorf("get inode failed: %s", getInodeResponse.GetStatusCode().String())
	}
	inode := getInodeResponse.GetInode()
	if len(inode.S3ChunkInfoMap) == 0 {
		row := make(map[string]string)
		row[ROW_FS_ID] = fmt.Sprintf("%d", inode.GetFsId())
		row[ROW_INODE_ID] = fmt.Sprintf("%d", inode.GetInodeId())
		row[ROW_LENGTH] = fmt.Sprintf("%d", inode.GetLength())
		row[ROW_TYPE] = inode.GetType().String()
		row[ROW_NLINK] = fmt.Sprintf("%d", inode.GetNlink())
		row[ROW_PARENT] = fmt.Sprintf("%d", inode.GetParent())
		iCmd.Table.AddRow(row)
	} else {
		rows := make([]map[string]string, 0)
		infoMap := inode.GetS3ChunkInfoMap()
		iCmd.Table.AddColumn(ROW_S3CHUNKINFO_CHUNKID)
		iCmd.Table.AddColumn(ROW_S3CHUNKINFO_OFFSET)
		iCmd.Table.AddColumn(ROW_S3CHUNKINFO_LENGTH)
		iCmd.Table.AddColumn(ROW_S3CHUNKINFO_SIZE)
		for _, infoList := range infoMap {
			for _, info := range infoList.GetS3Chunks() {
				row := make(map[string]string)
				row[ROW_FS_ID] = fmt.Sprintf("%d", inode.GetFsId())
				row[ROW_INODE_ID] = fmt.Sprintf("%d", inode.GetInodeId())
				row[ROW_LENGTH] = fmt.Sprintf("%d", inode.GetLength())
				row[ROW_TYPE] = inode.GetType().String()
				row[ROW_NLINK] = fmt.Sprintf("%d", inode.GetNlink())
				row[ROW_PARENT] = fmt.Sprintf("%d", inode.GetParent())
				row[ROW_S3CHUNKINFO_CHUNKID] = fmt.Sprintf("%d", info.GetChunkId())
				row[ROW_S3CHUNKINFO_OFFSET] = fmt.Sprintf("%d", info.GetOffset())
				row[ROW_S3CHUNKINFO_LENGTH] = fmt.Sprintf("%d", info.GetLen())
				row[ROW_S3CHUNKINFO_SIZE] = fmt.Sprintf("%d", info.GetSize())
				rows = append(rows, row)
			}
		}
		iCmd.Table.AddRows(rows)
	}
	iCmd.Result = getInodeResponse
	iCmd.Error = cmderror.ErrSuccess()
	return nil
}

func (iCmd *InodeCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&iCmd.FinalCurveCmd, iCmd)
}

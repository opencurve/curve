package maydamagedvolumes

import (
	"fmt"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
)

const (
	Exam = "curve bs list may-damaged-vols"
)

type MayDamVolCmd struct {
	basecmd.FinalCurveCmd
	timeout    time.Duration
	retryTimes int32
	addrs      []string

	clusterPoolsetsInfo []*topology.PoolsetInfo
	listPoolsetsRpc     *ListPoolsetsRpc

	clusterPhyPoolsInfo  []*topology.PhysicalPoolInfo
	listPhyPoolsInPstRpc *ListPhyPoolsInPstRpc

	clusterZonesInfo []*topology.ZoneInfo
	listZonesRpc     *ListZonesRpc

	clusterServersInfo []*topology.ServerInfo
	listServersRpc     *ListServersRpc

	listChunkServersRpc *ListChunkServersRpc
	chunkServerInfos    []*topology.ChunkServerInfo
	offLineCsAddrs      []string

	getChunkInfoRpc             *GetChunkInfoRpc
	getCopySetsInChunkServerRpc *GetCopySetsInChunkServerRpc

	copysetInfos []*common.CopysetInfo
	copysetsCnt  map[uint32]int
	damagedCps   []*common.CopysetInfo

	listVolsOnCpysRpc *ListVolumesOnCopysetsRpc
	fileNames         []string
	isRetired         bool

	rows []map[string]string
}

func NewMayDamVolCmd() *cobra.Command {
	mdvCmd := &MayDamVolCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "may-damaged-vols",
			Short:   "list may damaged volumes in curve bs",
			Example: Exam,
		},
	}
	basecmd.NewFinalCurveCli(&mdvCmd.FinalCurveCmd, mdvCmd)
	return mdvCmd.Cmd
}

var _ basecmd.FinalCurveCmdFunc = (*MayDamVolCmd)(nil) // check interface

func (mdvCmd *MayDamVolCmd) AddFlags() {
	config.AddRpcRetryTimesFlag(mdvCmd.Cmd)
	config.AddRpcTimeoutFlag(mdvCmd.Cmd)
	config.AddBsMdsFlagOption(mdvCmd.Cmd)
}

func (mdvCmd *MayDamVolCmd) InitRpcCall() error {
	addrs, addrErr := config.GetBsMdsAddrSlice(mdvCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	mdvCmd.addrs = addrs
	mdvCmd.timeout = config.GetBsFlagDuration(mdvCmd.Cmd, config.RPCTIMEOUT)
	mdvCmd.retryTimes = config.GetBsFlagInt32(mdvCmd.Cmd, config.RPCRETRYTIMES)
	return nil
}

func (mdvCmd *MayDamVolCmd) Init(cmd *cobra.Command, args []string) error {
	mdvCmd.isRetired = false
	mdvCmd.copysetsCnt = make(map[uint32]int)
	mdvCmd.InitRpcCall()
	// list all servers in cluster
	err := mdvCmd.GetServersInfo()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	// list all chunkservers on each servers
	err1 := mdvCmd.ListChunkServersOnServers()
	if err1.TypeCode() != cmderror.CODE_SUCCESS {
		return err1.ToError()
	}
	// find all offline chunkservers and record them in mdvCmd.offLineCsAddrs
	err = mdvCmd.FindOffLineChunkServers()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	if len(mdvCmd.offLineCsAddrs) == 0 {
		return err.ToError()
	}
	// get all copysets on offline servers
	err = mdvCmd.GetCopySetsOnOffLineCs()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	// Count the number of occurrences of each copyset, and 2 or more times are regarded as damaged copysets
	if !mdvCmd.CountOffLineCopysets() {
		fmt.Println("copysets are avaliable, no damaged volumes")
	}

	header := []string{cobrautil.ROW_FILE_NAME}
	mdvCmd.SetHeader(header)
	mdvCmd.TableNew.SetAutoMergeCellsByColumnIndex(cobrautil.GetIndexSlice(
		mdvCmd.Header, []string{cobrautil.ROW_FILE_NAME},
	))

	return err.ToError()
}

func (mdvCmd *MayDamVolCmd) RunCommand(cmd *cobra.Command, args []string) error {
	// List all volumes on damaged copysets
	err := mdvCmd.ListVolsOnDamagedCps()
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}
	for _, fileName := range mdvCmd.fileNames {
		row := make(map[string]string)
		row[cobrautil.ROW_FILE_NAME] = fileName
		mdvCmd.rows = append(mdvCmd.rows, row)
	}
	list := cobrautil.ListMap2ListSortByKeys(mdvCmd.rows, mdvCmd.Header, []string{
		cobrautil.ROW_FILE_NAME,
	})
	mdvCmd.TableNew.AppendBulk(list)
	return err.ToError()
}

func (mdvCmd *MayDamVolCmd) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&mdvCmd.FinalCurveCmd, mdvCmd)
}

func (mdvCmd *MayDamVolCmd) ResultPlainOutput() error {
	if mdvCmd.isRetired && len(mdvCmd.chunkServerInfos) == 0 {
		fmt.Println("all chunk servers retired")
		return nil
	}
	if len(mdvCmd.offLineCsAddrs) == 0 {
		fmt.Println("There is no off line chunkserver")
		return nil
	}
	return output.FinalCmdOutputPlain(&mdvCmd.FinalCurveCmd)
}

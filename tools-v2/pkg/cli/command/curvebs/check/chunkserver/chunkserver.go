package chunkserver

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
)

const (
	Exam = `curve bs check chunkserver --chunkserverId=1  
	        or
	        curve bs check chunkserver --chunkserverAddr=127.0.0.1:8200`
)

const (
	STATE_TRANSFERRING = "TRANSFERRING"
	STATE_CANDIDATE    = "CANDIDATE"
	kCpsHealthy        = 0  // all copysets on chunkserver are healthy
	kNotHealthy        = -1 // some copysets on chunkserver are not healthy
	kNotOnline         = -2 // chunkserver is not online
)

type ChunkServerCmd struct {
	basecmd.FinalCurveCmd
	timeout    time.Duration
	retryTimes int32
	addrs      []string

	csId       uint32
	csAddr     string
	csAddrFlag bool
	margin     int32
	detail     bool

	getChunkServerInfoRpc *GetChunkServerInfoRpc

	// this map is used to record the visited chunkserver and the copysets on it
	// key is a chunkserver address, values are copysets on the chunkserver
	visitedCs2GroupIds map[string][]string

	getCopySetsInChunkServerRpc *GetCopySetsInChunkServerRpc
	cs2CpyInfos                 map[string][]*common.CopysetInfo
	getCksInCopySetsRpc         *GetChunkServerListInCopySetsRpc

	// record the groupIds whose minor peers are not online
	minorPeersNotOnline []string
	// record the groupIds whose peers are not sufficient
	peersNotSufficient []string
	// record the groupIds whose major peers are not online
	majorPeersNotOnline []string
	logIndexGapTooBig   []string
	installingSnapshot  []string

	total             []string
	unhealthyCopysets []string
	abnormalHosts     []string

	noLeaderCps []string

	rows []map[string]string
}

func NewChunkServerCmd() *cobra.Command {
	cksCmd := &ChunkServerCmd{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "chunkserver",
			Short:   "check the health status of a chunkserver",
			Example: Exam,
		},
	}
	basecmd.NewFinalCurveCli(&cksCmd.FinalCurveCmd, cksCmd)
	return cksCmd.Cmd
}

var _ basecmd.FinalCurveCmdFunc = (*ChunkServerCmd)(nil) // check interface

func (cksCmd *ChunkServerCmd) AddFlags() {
	config.AddBsMdsFlagOption(cksCmd.Cmd)
	config.AddRpcRetryTimesFlag(cksCmd.Cmd)
	config.AddRpcTimeoutFlag(cksCmd.Cmd)
	config.AddBsChunkServerIdFlag(cksCmd.Cmd)
	config.AddBsChunkServerAddrFlag(cksCmd.Cmd)
	config.AddBsMarginFlag(cksCmd.Cmd)
	config.AddBsDetailFlag(cksCmd.Cmd)
}
func InitBaseRpcCall(cksCmd *ChunkServerCmd) error {
	addrs, addrErr := config.GetBsMdsAddrSlice(cksCmd.Cmd)
	if addrErr.TypeCode() != cmderror.CODE_SUCCESS {
		return fmt.Errorf(addrErr.Message)
	}
	cksCmd.addrs = addrs
	cksCmd.timeout = config.GetBsFlagDuration(cksCmd.Cmd, config.RPCTIMEOUT)
	cksCmd.retryTimes = config.GetBsFlagInt32(cksCmd.Cmd, config.RPCRETRYTIMES)
	return nil
}
func (cksCmd *ChunkServerCmd) Init(cmd *cobra.Command, args []string) error {
	InitBaseRpcCall(cksCmd)
	cksCmd.csAddrFlag = false
	cksCmd.visitedCs2GroupIds = make(map[string][]string)
	cksCmd.cs2CpyInfos = make(map[string][]*common.CopysetInfo)

	cksCmd.margin = config.GetBsFlagInt32(cksCmd.Cmd, config.CURVEBS_MARGIN)
	cksCmd.detail = config.GetBsFlagBool(cksCmd.Cmd, config.CURVEBS_DETAIL)

	header := []string{"total copysets", "unhealthy copysets", "unhealthy ratio"}
	cksCmd.SetHeader(header)

	if (cmd.Flag(config.CURVEBS_CHUNKSERVER_ID).Changed) && (cmd.Flag(config.CURVEBS_CHUNKSERVER_ADDR).Changed) {
		return fmt.Errorf("csId and csAddr shouldn't be set at the same time")
	}
	if cmd.Flag(config.CURVEBS_CHUNKSERVER_ID).Changed {
		id := config.GetBsFlagString(cksCmd.Cmd, config.CURVEBS_CHUNKSERVER_ID)
		t, _ := strconv.ParseUint(id, 10, 32)
		cksCmd.csId = uint32(t)
		err, csAddr := cksCmd.GetCsAddrBycsId(cksCmd.csId)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err.ToError()
		}
		cksCmd.csAddr = csAddr
		return err.ToError()
	} else if cmd.Flag(config.CURVEBS_CHUNKSERVER_ADDR).Changed {
		cksCmd.csAddr = config.GetBsFlagString(cksCmd.Cmd, config.CURVEBS_CHUNKSERVER_ADDR)
		res := strings.Split(cksCmd.csAddr, ":")
		if len(res) < 2 {
			return fmt.Errorf("address is invalid, please set --csAddr=$ip:$port")
		}
		if !cobrautil.IsValidIp(res[0]) && !cobrautil.IsValidPort(res[1]) {
			return fmt.Errorf("please check the validity of ip and port")
		}
		cksCmd.csAddrFlag = true
	} else {
		return fmt.Errorf("one of csId and csAddr must be set")
	}
	return cmderror.ErrSuccess().ToError()
}

func (cksCmd *ChunkServerCmd) RunCommand(cmd *cobra.Command, args []string) error {
	_, res := cksCmd.CheckCpsOnCksBycsAddr(cksCmd.csAddr, []string{})
	if res == kCpsHealthy {
		fmt.Println("queryed chunkServer is healthy")
		fmt.Println()
	} else if res == kNotOnline {
		fmt.Println("queryed chunkserver is not online")
		fmt.Println()
	} else if res == kNotHealthy {
		fmt.Println("queryed chunkserver is not healthy")
		fmt.Println()
	}
	unhealthyCnt, r := cksCmd.CountUnHealthyCopysets()
	row := make(map[string]string)
	total := strconv.FormatUint(uint64(len(cksCmd.total)), 10)
	unCnt := strconv.FormatUint(uint64(unhealthyCnt), 10)
	ratio := strconv.FormatFloat(r, 'f', 2, 64) + "%"
	row[cobrautil.ROW_TOTAL_COPYSETS] = total
	row[cobrautil.ROW_UNHEALTHY_COPYSETS] = unCnt
	row[cobrautil.ROW_UNHEALTHY_RATIO] = ratio
	cksCmd.rows = append(cksCmd.rows, row)
	list := cobrautil.ListMap2ListSortByKeys(cksCmd.rows, cksCmd.Header, []string{})
	cksCmd.TableNew.AppendBulk(list)
	if cksCmd.detail {
		cksCmd.PrintExceptionInfo()
	}
	return cmderror.ErrSuccess().ToError()
}

func (cksCmd *ChunkServerCmd) CountUnHealthyCopysets() (int, float64) {
	if len(cksCmd.minorPeersNotOnline) != 0 {
		for _, gid := range cksCmd.minorPeersNotOnline {
			index := slices.Index(cksCmd.unhealthyCopysets, gid)
			if index == -1 {
				cksCmd.unhealthyCopysets = append(cksCmd.unhealthyCopysets, gid)
			}
		}
	}
	if len(cksCmd.peersNotSufficient) != 0 {
		for _, gid := range cksCmd.peersNotSufficient {
			index := slices.Index(cksCmd.unhealthyCopysets, gid)
			if index == -1 {
				cksCmd.unhealthyCopysets = append(cksCmd.unhealthyCopysets, gid)
			}
		}
	}
	if len(cksCmd.majorPeersNotOnline) != 0 {
		for _, gid := range cksCmd.majorPeersNotOnline {
			index := slices.Index(cksCmd.unhealthyCopysets, gid)
			if index == -1 {
				cksCmd.unhealthyCopysets = append(cksCmd.unhealthyCopysets, gid)
			}
		}
	}
	if len(cksCmd.logIndexGapTooBig) != 0 {
		for _, gid := range cksCmd.logIndexGapTooBig {
			index := slices.Index(cksCmd.unhealthyCopysets, gid)
			if index == -1 {
				cksCmd.unhealthyCopysets = append(cksCmd.unhealthyCopysets, gid)
			}
		}
	}
	if len(cksCmd.installingSnapshot) != 0 {
		for _, gid := range cksCmd.installingSnapshot {
			index := slices.Index(cksCmd.unhealthyCopysets, gid)
			if index == -1 {
				cksCmd.unhealthyCopysets = append(cksCmd.unhealthyCopysets, gid)
			}
		}
	}
	if len(cksCmd.noLeaderCps) != 0 {
		for _, gid := range cksCmd.noLeaderCps {
			index := slices.Index(cksCmd.unhealthyCopysets, gid)
			if index == -1 {
				cksCmd.unhealthyCopysets = append(cksCmd.unhealthyCopysets, gid)
			}
		}
	}
	unhealthyCnt := len(cksCmd.unhealthyCopysets)
	unhealthyRatio := float64(len(cksCmd.unhealthyCopysets)) / float64(len(cksCmd.total))
	unhealthyRatio = unhealthyRatio * 100
	return unhealthyCnt, unhealthyRatio

}

func (cksCmd *ChunkServerCmd) PrintExceptionInfo() {
	if len(cksCmd.abnormalHosts) != 0 {
		fmt.Println("abnormal hosts:")
		for _, host := range cksCmd.abnormalHosts {
			fmt.Println(host)
		}
		fmt.Println()
	}
	if len(cksCmd.minorPeersNotOnline) != 0 {
		fmt.Println(len(cksCmd.minorPeersNotOnline), "copysets in which minor peers are not online")
		fmt.Println("the detail info is:")
		fmt.Println("  groupId  ", "logicalPoolId", "copysetId")
		for _, gid := range cksCmd.minorPeersNotOnline {
			gid_uint, _ := strconv.ParseUint(gid, 10, 64)
			pid := cobrautil.GetPoolIdFromGid(gid_uint)
			cid := cobrautil.GetCpsIdFromGid(gid_uint)
			fmt.Println(gid_uint, "       ", pid, "        ", cid)
		}
		fmt.Println()
	}
	if len(cksCmd.peersNotSufficient) != 0 {
		fmt.Println(len(cksCmd.peersNotSufficient), "copysets in which peers amount not enough")
		fmt.Println("the detail info is:")
		fmt.Println("  groupId  ", "logicalPoolId", "copysetId")
		for _, gid := range cksCmd.peersNotSufficient {
			gid_uint, _ := strconv.ParseUint(gid, 10, 64)
			pid := cobrautil.GetPoolIdFromGid(gid_uint)
			cid := cobrautil.GetCpsIdFromGid(gid_uint)
			fmt.Println(gid_uint, "       ", pid, "        ", cid)
		}
		fmt.Println()
	}
	if len(cksCmd.majorPeersNotOnline) != 0 {
		fmt.Println(len(cksCmd.majorPeersNotOnline), "copysets in which major peers are not online")
		fmt.Println("the detail info is:")
		fmt.Println("  groupId  ", "logicalPoolId", "copysetId")
		for _, gid := range cksCmd.majorPeersNotOnline {
			gid_uint, _ := strconv.ParseUint(gid, 10, 64)
			pid := cobrautil.GetPoolIdFromGid(gid_uint)
			cid := cobrautil.GetCpsIdFromGid(gid_uint)
			fmt.Println(gid_uint, "       ", pid, "        ", cid)
		}
		fmt.Println()
	}
	if len(cksCmd.logIndexGapTooBig) != 0 {
		fmt.Println(len(cksCmd.logIndexGapTooBig), "copysets in which log index gap too big")
		fmt.Println("the detail info is:")
		fmt.Println("  groupId  ", "logicalPoolId", "copysetId")
		for _, gid := range cksCmd.logIndexGapTooBig {
			gid_uint, _ := strconv.ParseUint(gid, 10, 64)
			pid := cobrautil.GetPoolIdFromGid(gid_uint)
			cid := cobrautil.GetCpsIdFromGid(gid_uint)
			fmt.Println(gid_uint, "       ", pid, "        ", cid)
		}
		fmt.Println()
	}
	if len(cksCmd.installingSnapshot) != 0 {
		fmt.Println(len(cksCmd.installingSnapshot), "copysets are installing sanpshot")
		fmt.Println("the detail info is:")
		fmt.Println("  groupId  ", "logicalPoolId", "copysetId")
		for _, gid := range cksCmd.installingSnapshot {
			gid_uint, _ := strconv.ParseUint(gid, 10, 64)
			pid := cobrautil.GetPoolIdFromGid(gid_uint)
			cid := cobrautil.GetCpsIdFromGid(gid_uint)
			fmt.Println(gid_uint, "       ", pid, "        ", cid)
		}
		fmt.Println()
	}
	if len(cksCmd.noLeaderCps) != 0 {
		fmt.Println(len(cksCmd.noLeaderCps), "copysets have no leader")
		fmt.Println("the detail info is:")
		fmt.Println("  groupId  ", "logicalPoolId", "copysetId")
		for _, gid := range cksCmd.noLeaderCps {
			gid_uint, _ := strconv.ParseUint(gid, 10, 64)
			pid := cobrautil.GetPoolIdFromGid(gid_uint)
			cid := cobrautil.GetCpsIdFromGid(gid_uint)
			fmt.Println(gid_uint, "       ", pid, "        ", cid)
		}
		fmt.Println()
	}
}

func (cksCmd *ChunkServerCmd) CheckHealthOnLeader(cpsInfo map[string]string) int {
	peers := cpsInfo[PEERS]
	cnt := strings.Split(peers, " ")
	if len(cnt) < 3 {
		return kPeersNoSufficient
	}
	var peersAddrs []string
	for _, peer := range cnt {
		res := strings.Split(peer, ":")
		if len(res) != 3 {
			fmt.Println("peer address is invalid")
			return -1
		}
		peerAddr := res[0] + ":" + res[1]
		peersAddrs = append(peersAddrs, peerAddr)
	}
	gid := cpsInfo[GROUPID]
	checkRes := cksCmd.CheckPeersOnLineStatus(gid, peersAddrs)
	if checkRes != kHealthy {
		return checkRes
	}
	lastLogId := cpsInfo[LAST_LOG_ID]
	index := ParseLastLogId(lastLogId)
	if index == "" {
		fmt.Println("ParseLastLogId fail")
		return kParseError
	}
	idx, _ := strconv.ParseInt(index, 10, 64)
	repInfo1 := cpsInfo[REPLICATOR+"1"]
	repInfo2 := cpsInfo[REPLICATOR+"2"]
	n1, f1, s1 := ParseReplicatorData(repInfo1)
	if s1 != "" {
		return kInstallingSnapshot
	}
	if n1 == "" {
		fmt.Println("Parse Replicator1 Data fail, next_index is null")
		return kParseError
	}
	if f1 == "" {
		fmt.Println("Parse Replicator1 Data fail, flying_append_entries_size is null")
		return kParseError
	}

	n2, f2, s2 := ParseReplicatorData(repInfo2)
	if s2 != "" {
		return kInstallingSnapshot
	}
	if n2 == "" {
		fmt.Println("Parse Replicator2 Data fail, next_index is null")
		return kParseError
	}
	if f2 == "" {
		fmt.Println("Parse Replicator2 Data fail, flying_append_entries_size is null")
		return kParseError
	}

	nextIndex1, _ := strconv.ParseInt(n1, 10, 64)
	flying1, _ := strconv.ParseInt(f1, 10, 64)
	nextIndex2, _ := strconv.ParseInt(n2, 10, 64)
	flying2, _ := strconv.ParseInt(f2, 10, 64)
	gap := 0
	gap1 := idx - (nextIndex1 - 1 - flying1)
	gap2 := idx - (nextIndex2 - 1 - flying2)
	if gap1 > gap2 {
		gap = int(gap1)
	} else {
		gap = int(gap2)
	}
	if gap > int(cksCmd.margin) {
		return kLogIndexGapTooBig
	}

	return kHealthy
}

// when gids is  nil, check all copysets on the chunkserver,
// but when gid is not nil, only check the leader copysets
func (cksCmd *ChunkServerCmd) CheckCpsOnCksBycsAddr(csAddr string, gids []string) (*cmderror.CmdError, int) {
	isHealthy := true
	err, raft_stat_info := cksCmd.GetAndParseCpsData(csAddr)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		cksCmd.RecordOffLineCps(csAddr)
		return err, kNotOnline
	}

	// record mappings of chunkserver to groupId
	// key is chunkserver's addr, value is groupId list
	csAddrMap := make(map[string][]string)
	noLeaderCopysetsPeers := make(map[string][]string)
	// record peers of copysets of no leader, key is groupId value is configuration
	if len(raft_stat_info) == 0 {
		return cmderror.ErrChunkServerNotHealth(), kNotHealthy
	}
	if len(gids) == 0 {
		for _, m := range raft_stat_info {
			cksCmd.visitedCs2GroupIds[csAddr] = append(cksCmd.visitedCs2GroupIds[csAddr], m[GROUPID])
		}
	}
	for _, m := range raft_stat_info {
		index := slices.Index(cksCmd.total, m[GROUPID])
		if index == -1 {
			cksCmd.total = append(cksCmd.total, m[GROUPID])
		}
		if m[STATE] == "LEADER" {
			chekRes := cksCmd.CheckHealthOnLeader(m)
			switch chekRes {
			case kPeersNoSufficient:
				cksCmd.peersNotSufficient = append(cksCmd.peersNotSufficient, m[GROUPID])
				isHealthy = false
			case kLogIndexGapTooBig:
				cksCmd.logIndexGapTooBig = append(cksCmd.logIndexGapTooBig, m[GROUPID])
				isHealthy = false
			case kInstallingSnapshot:
				cksCmd.installingSnapshot = append(cksCmd.installingSnapshot, m[GROUPID])
				isHealthy = false
			case kMinorityPeerNotOnline:
				cksCmd.minorPeersNotOnline = append(cksCmd.minorPeersNotOnline, m[GROUPID])
				isHealthy = false
			case kMajorityPeerNotOnline:
				cksCmd.majorPeersNotOnline = append(cksCmd.majorPeersNotOnline, m[GROUPID])
				isHealthy = false
			case kParseError:
				isHealthy = false
			default:
				isHealthy = true
			}
		} else if len(gids) == 0 && m[STATE] == "FOLLOWER" {
			gid := m[GROUPID]
			// if the copyset leader is not online, record and then query its peers status
			res := strings.Split(m[LEADER], ".")
			if res[0] == "0" {
				peersAddrs := strings.Split(m[PEERS], " ")
				noLeaderCopysetsPeers[gid] = append(noLeaderCopysetsPeers[gid], peersAddrs...)
				continue
			} else {
				// if the copyset leader is online, record the leader address
				res := strings.Split(m[LEADER], ":")
				lAddr := res[0] + ":" + res[1]
				index := slices.Index(csAddrMap[lAddr], gid)
				if index == -1 {
					csAddrMap[lAddr] = append(csAddrMap[lAddr], gid)
				}
			}

		} else if len(gids) == 0 && m[STATE] == STATE_CANDIDATE || m[STATE] == STATE_TRANSFERRING {
			cksCmd.noLeaderCps = append(cksCmd.noLeaderCps, m[GROUPID])
			isHealthy = false
		} else if len(gids) == 0 && m[STATE] != "LEADER" && m[STATE] != "FOLLOWER" && m[STATE] != STATE_CANDIDATE && m[STATE] != STATE_TRANSFERRING {
			fmt.Println(m[GROUPID], "state is abnormal, state is:", m[STATE])
			isHealthy = false
		}
	}
	err, health := cksCmd.CheckCopysetsNoLeader(csAddr, noLeaderCopysetsPeers) // cksCmd.noLeaderCopysetsPeers
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err, kNotHealthy
	}
	if !health {
		isHealthy = false
	}
	for k, v := range csAddrMap {
		err, res := cksCmd.CheckCpsOnCksBycsAddr(k, v)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err, kNotHealthy
		}
		if res != kHealthy {
			isHealthy = false
		}
	}

	if !isHealthy {
		return cmderror.ErrSuccess(), kNotHealthy
	} else {
		return cmderror.ErrSuccess(), kCpsHealthy
	}
}

// need data: cksCmd.csAdd, cksCmd.noLeaderCopysetsPeers
func (cksCmd *ChunkServerCmd) CheckCopysetsNoLeader(csAddr string, noLeaderCopysetsPeers map[string][]string) (*cmderror.CmdError, bool) {
	if len(noLeaderCopysetsPeers) == 0 {
		return cmderror.ErrSuccess(), true
	}
	isHealthy := true
	var gids []string
	for k, _ := range noLeaderCopysetsPeers {
		gids = append(gids, k)
	}
	err, result := cksCmd.CheckIfChunkServerInCopysets(csAddr, gids)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err, false
	}
	for k, v := range result {
		if v {
			isHealthy = false
			gid := k
			pAddrs := noLeaderCopysetsPeers[gid]
			var peersAddrs []string
			for _, addr := range pAddrs {
				res := strings.Split(addr, ":")
				peerAddr := res[0] + ":" + res[1]
				peersAddrs = append(peersAddrs, peerAddr)
			}
			checkRes := cksCmd.CheckPeersOnLineStatus(gid, peersAddrs)
			if checkRes == kMajorityPeerNotOnline {
				cksCmd.majorPeersNotOnline = append(cksCmd.majorPeersNotOnline, gid)
				continue
			}
			cksCmd.noLeaderCps = append(cksCmd.noLeaderCps, gid)
		}
	}
	return cmderror.ErrSuccess(), isHealthy
}

func (cksCmd *ChunkServerCmd) CheckIfChunkServerInCopysets(csAddr string, gids []string) (*cmderror.CmdError, map[string]bool) {
	lpid2cpsId := make(map[uint32][]uint32)
	for _, gid := range gids {
		gid_i, _ := strconv.ParseUint(gid, 10, 64)
		lpid := cobrautil.GetPoolIdFromGid(gid_i)
		cpsId := cobrautil.GetCpsIdFromGid(gid_i)
		lpid2cpsId[lpid] = append(lpid2cpsId[lpid], cpsId)
	}
	result := make(map[string]bool)
	for k, v := range lpid2cpsId {
		err, cpsServerInfos := cksCmd.GetChunkServerListOfCopySets(k, v)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err, result
		}
		for _, info := range cpsServerInfos {
			cid := info.GetCopysetId()
			groupId := cobrautil.GenGroupId(k, cid)
			for _, csLoc := range info.GetCsLocs() {
				p := csLoc.GetPort()
				port := strconv.FormatUint(uint64(p), 10)
				addr := csLoc.GetHostIp() + ":" + port
				if addr == csAddr {
					groupId_s := strconv.FormatUint(groupId, 10)
					result[groupId_s] = true
					break
				}
			}
		}
	}
	return cmderror.ErrSuccess(), result
}

func (cksCmd *ChunkServerCmd) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&cksCmd.FinalCurveCmd, cksCmd)
}

func (cksCmd *ChunkServerCmd) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&cksCmd.FinalCurveCmd)
}

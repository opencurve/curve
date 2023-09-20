/*
*  Copyright (c) 2023 NetEase Inc.
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
* Created Date: 2023-08-29
* Author: caoxianfei1
 */

package copyset

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	set "github.com/deckarep/golang-set/v2"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	chunkserver "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/chunkserver"
	copyset "github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs/list/copyset"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

type ChunkServerLocation struct {
	ChunkServerId uint32 `json:"chunkServerId" binding:"required"`
	HostIp        string `json:"hostIp" binding:"required"`
	Port          uint32 `josn:"port" binding:"required"`
	ExternalIp    string `json:"externalIp"`
}

type CopySetServerInfo struct {
	CopysetId uint32                `json:"copysetId" binding:"required"`
	CsLocs    []ChunkServerLocation `json:"csLocs" binding:"required"`
}

type CopySetInfo struct {
	LogicalPoolId      uint32 `json:"logicalPoolId" binding:"required"`
	CopysetId          uint32 `json:"copysetId" binding:"required"`
	Scanning           bool   `json:"scanning"`
	LastScanSec        uint64 `json:"lastScanSec"`
	LastScanConsistent bool   `json:"lastScanConsistent"`
}

type GetChunkServerListInCopySetsRPC struct {
	Info                  *basecmd.Rpc
	Request               *topology.GetChunkServerListInCopySetsRequest
	topologyServiceClient topology.TopologyServiceClient
}

var _ basecmd.RpcFunc = (*GetChunkServerListInCopySetsRPC)(nil) // check interface

func (lRpc *GetChunkServerListInCopySetsRPC) NewRpcClient(cc grpc.ClientConnInterface) {
	lRpc.topologyServiceClient = topology.NewTopologyServiceClient(cc)
}

func (lRpc *GetChunkServerListInCopySetsRPC) Stub_Func(ctx context.Context) (interface{}, error) {
	return lRpc.topologyServiceClient.GetChunkServerListInCopySets(ctx, lRpc.Request)
}

func GetChunkServerListInCopySets(logicalPoolId uint32, copysetIds []uint32, cmd *cobra.Command) ([]CopySetServerInfo, error) {
	Rpc := &GetChunkServerListInCopySetsRPC{}
	addrs, err := config.GetBsMdsAddrSlice(cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err.ToError()
	}
	Rpc.Info = basecmd.NewRpc(addrs, config.GetRpcTimeout(cmd), 3, "GetChunkServerListInCopySets")
	Rpc.Request = &topology.GetChunkServerListInCopySetsRequest{}
	Rpc.Request.LogicalPoolId = &logicalPoolId
	Rpc.Request.CopysetId = append(Rpc.Request.CopysetId, copysetIds...)
	result, err := basecmd.GetRpcResponse(Rpc.Info, Rpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err.ToError()
	}

	response := result.(*topology.GetChunkServerListInCopySetsResponse)
	statusCode := response.GetStatusCode()
	if statusCode != 0 {
		return nil, fmt.Errorf("GetChunkServerListInCopySets failed, and rpc code is %d", statusCode)
	}

	infos := []CopySetServerInfo{}
	for _, csInfo := range response.GetCsInfo() {
		info := CopySetServerInfo{}
		info.CopysetId = csInfo.GetCopysetId()
		for _, locs := range csInfo.GetCsLocs() {
			var l ChunkServerLocation
			l.ChunkServerId = locs.GetChunkServerID()
			l.HostIp = locs.GetHostIp()
			l.Port = locs.GetPort()
			l.ExternalIp = locs.GetExternalIp()
			info.CsLocs = append(info.CsLocs, l)
		}
		infos = append(infos, info)
	}

	return infos, nil
}

const (
	// copyset status check result
	COPYSET_CHECK_HEALTHY               = "healthy"
	COPYSET_CHECK_PARSE_ERROR           = "parse_error" //
	COPYSET_CHECK_NO_LEADER             = "no leader"
	COPYSET_CHECK_PEERS_NO_SUFFICIENT   = "peers_no_sufficient"   //
	COPYSET_CHECK_LOG_INDEX_TOO_BIG     = "log_index_gap_too_big" //
	COPYSET_CHECK_INSTALLING_SNAPSHOT   = "installing_snapshot"   //
	COPYSET_CHECK_MINORITY_PEER_OFFLINE = "minority_peer_offline" //
	COPYSET_CHECK_MAJORITY_PEER_OFFLINE = "majority_peer_offline" //
	COPYSET_CHECK_INCONSISTENT          = "Three copies inconsistent"
	COPYSET_CHECK_OTHER_ERROR           = "other_error"
	COPYSET_TOTAL                       = "total"
)

type Copyset struct {
	// recored copysets in all status; key is status, value is groupIds
	copyset map[string]set.Set[string]
	// record chunkservers which send rpc failed
	serviceExceptionChunkservers set.Set[string]
	// record chunkservers which load copyset error
	copysetLoacExceptionChunkServers set.Set[string]
	// record copysets which belongs to different chunkservers, chunkserverEndpoint <-> copysets
	chunkServerCopysets map[string]set.Set[string]
	// recode coopyset which stat(ok, warn, error) to groupid
	copysetStat map[int32]set.Set[string]
}

func NewCopyset() *Copyset {
	var cs Copyset
	cs.copyset = make(map[string]set.Set[string])
	cs.serviceExceptionChunkservers = set.NewSet[string]()
	cs.copysetLoacExceptionChunkServers = set.NewSet[string]()
	cs.chunkServerCopysets = make(map[string]set.Set[string])
	cs.copysetStat = make(map[int32]set.Set[string])
	return &cs
}

func getGroupId(poolId, copysetId uint32) uint64 {
	return uint64(poolId)<<32 | uint64(copysetId)
}

func getPoolIdFormGroupId(groupId uint64) uint32 {
	return uint32(groupId >> 32)
}

func getCopysetIdFromGroupId(groupId uint64) uint32 {
	return uint32(groupId & (uint64(1)<<32 - 1))
}

func (cs *Copyset) recordCopyset(s, groupId string) {
	if _, ok := cs.copyset[s]; !ok {
		cs.copyset[s] = set.NewSet[string]()
	}
	cs.copyset[s].Add(groupId)
}

func (cs *Copyset) recordCopysetStat(state int32, groupId string) {
	if _, ok := cs.copysetStat[state]; !ok {
		cs.copysetStat[state] = set.NewSet[string]()
	}
	cs.copysetStat[state].Add(groupId)
}

func (cs *Copyset) checkCopysetOnline(addr, groupId string) bool {
	copysets := cs.chunkServerCopysets[addr]
	if copysets != nil {
		return copysets.Contains(groupId)
	}
	return false
}

func (cs *Copyset) checkPeerOnlineStatus(groupId string, peers []string) string {
	offlineNum := 0
	for _, peer := range peers {
		addr := peer[0:strings.LastIndex(peer, ":")]
		online := cs.checkCopysetOnline(addr, groupId)
		if !online {
			offlineNum += 1
		}
	}
	if offlineNum > 0 {
		if offlineNum < len(peers)/2+1 {
			return COPYSET_CHECK_MINORITY_PEER_OFFLINE
		} else {
			return COPYSET_CHECK_MAJORITY_PEER_OFFLINE
		}
	}
	return COPYSET_CHECK_HEALTHY
}

func (cs *Copyset) updatePeerOfflineCopysets(addr string, id uint32, cmd *cobra.Command) error {
	// get copysets on offline chunkserver from mds server
	cmd.Flags().Set(config.CURVEBS_CHUNKSERVER_ID, strconv.FormatUint(uint64(id), 10))
	copysets, err := chunkserver.GetCopySetsInChunkServer(cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	// get copyset's all members
	var logicalPoolId uint32
	var copysetIds []uint32
	for _, copyset := range copysets {
		copysetIds = append(copysetIds, *copyset.CopysetId)
	}
	if len(copysets) > 0 {
		logicalPoolId = *copysets[0].LogicalPoolId
	}
	memberInfo, err2 := GetChunkServerListInCopySets(logicalPoolId, copysetIds, cmd)
	if err2 != nil {
		return err2
	}
	// check all members
	for _, info := range memberInfo {
		var peers []string
		for _, loc := range info.CsLocs {
			endpoint := fmt.Sprintf("%s:%d:0", loc.HostIp, loc.Port)
			peers = append(peers, endpoint)
		}
		groupId := strconv.FormatUint(getGroupId(logicalPoolId, info.CopysetId), 10)
		ret := cs.checkPeerOnlineStatus(groupId, peers)
		switch ret {
		case COPYSET_CHECK_MINORITY_PEER_OFFLINE:
			cs.recordCopyset(COPYSET_CHECK_MINORITY_PEER_OFFLINE, groupId)
			cs.recordCopysetStat(int32(cobrautil.COPYSET_WARN), groupId)
		case COPYSET_CHECK_MAJORITY_PEER_OFFLINE:
			cs.recordCopyset(COPYSET_CHECK_MAJORITY_PEER_OFFLINE, groupId)
			cs.recordCopysetStat(int32(cobrautil.COPYSET_WARN), groupId)
		default:
			cs.recordCopyset(COPYSET_TOTAL, groupId)
		}
	}
	return nil
}

func (cs *Copyset) updateChunkServerCopysets(addr string, status []map[string]string) {
	copysetGroupIds := set.NewSet[string]()
	for _, s := range status {
		copysetGroupIds.Add(s[RAFT_STATUS_KEY_GROUPID])
	}

	cs.chunkServerCopysets[addr] = copysetGroupIds
}

func (cs *Copyset) ifChunkServerInCopysets(csAddr string, groupIds *set.Set[string], cmd *cobra.Command) (map[string]bool, error) {
	var logicalPoolId uint32
	var copysetIds []uint32
	result := make(map[string]bool)
	for gid := range (*groupIds).Iter() {
		ngid, err := strconv.ParseUint(gid, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse string groupId to uint64 error: %s", gid)
		}
		logicalPoolId = getPoolIdFormGroupId(ngid)
		copysetIds = append(copysetIds, getCopysetIdFromGroupId(ngid))
	}
	memberInfo, err := GetChunkServerListInCopySets(logicalPoolId, copysetIds, cmd)
	if err != nil {
		return nil, fmt.Errorf("GetChunkServerListInCopySets failed, %s", err)
	}
	for _, info := range memberInfo {
		csId := info.CopysetId
		gId := getGroupId(logicalPoolId, csId)
		for _, csloc := range info.CsLocs {
			addr := fmt.Sprintf("%s:%d", csloc.HostIp, csloc.Port)
			if csAddr == addr {
				result[strconv.FormatUint(gId, 10)] = true
				break
			}
		}
	}
	return result, nil
}

func (cs *Copyset) checkCopysetsNoLeader(csAddr string, peersMap *map[string][]string, cmd *cobra.Command) bool {
	healthy := true
	if len(*peersMap) == 0 {
		return true
	}
	groupIds := set.NewSet[string]()
	for k := range *peersMap {
		groupIds.Add(k)
	}
	result, err := cs.ifChunkServerInCopysets(csAddr, &groupIds, cmd)
	if err != nil {
		return false
	}

	for k, v := range result {
		if v {
			healthy = false
			ret := cs.checkPeerOnlineStatus(k, (*peersMap)[k])
			if ret == COPYSET_CHECK_MAJORITY_PEER_OFFLINE {
				cs.recordCopyset(COPYSET_CHECK_MAJORITY_PEER_OFFLINE, k)
				// no leader copyset is always error
				cs.recordCopysetStat(int32(cobrautil.COPYSET_ERROR), k)
				continue
			}
			cs.recordCopyset(COPYSET_CHECK_NO_LEADER, k)
			cs.recordCopysetStat(int32(cobrautil.COPYSET_ERROR), k)
		}
	}
	return healthy
}

func (cs *Copyset) checkHealthOnLeader(raftStatus *map[string]string) string {
	// 1. check peers number
	peers, ok := (*raftStatus)[RAFT_STATUS_KEY_PEERS]
	if !ok {
		return COPYSET_CHECK_PARSE_ERROR
	}
	peerArray := strings.Split(peers, " ")
	if len(peerArray) < RAFT_REPLICAS_NUMBER {
		return COPYSET_CHECK_PEERS_NO_SUFFICIENT
	}

	// 2. check offline peers
	groupId := (*raftStatus)[RAFT_STATUS_KEY_GROUPID]
	ret := cs.checkPeerOnlineStatus(groupId, peerArray)
	if ret != COPYSET_CHECK_HEALTHY {
		return ret
	}

	// 3. check log index gap between replicas
	strLastLogId := (*raftStatus)[RAFT_STATUS_KEY_LAST_LOG_ID]
	lastLogId, err := strconv.ParseUint(strLastLogId[strings.Index(strLastLogId, "=")+1:strings.Index(strLastLogId, ",")], 10, 64)
	if err != nil {
		return COPYSET_CHECK_PARSE_ERROR
	}
	var gap, nextIndex, flying uint64
	gap = 0
	nextIndex = 0
	flying = 0
	for i, size := 0, len(peerArray)-1; i < size; i++ {
		key := fmt.Sprintf("%s%d", RAFT_STATUS_KEY_REPLICATOR, i)
		repInfos := strings.Split((*raftStatus)[key], " ")
		for _, info := range repInfos {
			if !strings.Contains(info, "=") {
				if strings.Contains(info, RAFT_STATUS_KEY_SNAPSHOT) {
					return COPYSET_CHECK_INSTALLING_SNAPSHOT
				} else {
					continue
				}
			}
			pos := strings.Index(info, "=")
			if info[0:pos] == RAFT_STATUS_KEY_NEXT_INDEX {
				nextIndex, err = strconv.ParseUint(info[pos+1:], 10, 64)
				if err != nil {
					return COPYSET_CHECK_PARSE_ERROR
				}
			} else if info[0:pos] == RAFT_STATUS_KEY_FLYING_APPEND_ENTRIES_SIZE {
				flying, err = strconv.ParseUint(info[pos+1:], 10, 64)
				if err != nil {
					return COPYSET_CHECK_PARSE_ERROR
				}
			}
			gap = MaxUint64(gap, lastLogId-(nextIndex-1-flying))
		}
	}
	if gap > RAFT_MARGIN {
		return COPYSET_CHECK_LOG_INDEX_TOO_BIG
	}
	return COPYSET_CHECK_HEALTHY
}

func (cs *Copyset) checkCopysetsOnChunkServer(addr string, status []map[string]string, addr2Id map[string]uint32, cmd *cobra.Command) bool {
	healthy := true
	// query copysets' status failed on chunkserver and think it offline
	if status == nil {
		cs.updatePeerOfflineCopysets(addr, addr2Id[addr], cmd)
		cs.serviceExceptionChunkservers.Add(addr)
		return false
	}

	if len(status) == 0 {
		cs.copysetLoacExceptionChunkServers.Add(addr)
		cs.serviceExceptionChunkservers.Add(addr)
		return false
	}

	noLeaderCopysetPeers := make(map[string][]string)
	for _, statMap := range status {
		groupId := statMap[RAFT_STATUS_KEY_GROUPID]
		state := statMap[RAFT_STATUS_KEY_STATE]
		cs.recordCopyset(COPYSET_TOTAL, groupId)
		// copyset is leader
		if state == RAFT_STATUS_STATE_LEADER {
			ret := cs.checkHealthOnLeader(&statMap)
			switch ret {
			case COPYSET_CHECK_PEERS_NO_SUFFICIENT:
				cs.recordCopyset(COPYSET_CHECK_PEERS_NO_SUFFICIENT, groupId)
				cs.recordCopysetStat(int32(cobrautil.COPYSET_WARN), groupId)
				healthy = false
			case COPYSET_CHECK_LOG_INDEX_TOO_BIG:
				cs.recordCopyset(COPYSET_CHECK_LOG_INDEX_TOO_BIG, groupId)
				cs.recordCopysetStat(int32(cobrautil.COPYSET_WARN), groupId)
				healthy = false
			case COPYSET_CHECK_INSTALLING_SNAPSHOT:
				cs.recordCopyset(COPYSET_CHECK_INSTALLING_SNAPSHOT, groupId)
				cs.recordCopysetStat(int32(cobrautil.COPYSET_WARN), groupId)
				healthy = false
			case COPYSET_CHECK_MINORITY_PEER_OFFLINE:
				cs.recordCopyset(COPYSET_CHECK_MINORITY_PEER_OFFLINE, groupId)
				cs.recordCopysetStat(int32(cobrautil.COPYSET_WARN), groupId)
				healthy = false
			case COPYSET_CHECK_MAJORITY_PEER_OFFLINE:
				cs.recordCopyset(COPYSET_CHECK_MAJORITY_PEER_OFFLINE, groupId)
				cs.recordCopysetStat(int32(cobrautil.COPYSET_ERROR), groupId)
				healthy = false
			case COPYSET_CHECK_PARSE_ERROR:
				cs.recordCopyset(COPYSET_CHECK_PARSE_ERROR, groupId)
				cs.recordCopysetStat(int32(cobrautil.COPYSET_WARN), groupId)
				healthy = false
			}
			// copyset is follower
		} else if state == RAFT_STATUS_STATE_FOLLOWER {
			v, ok := statMap[RAFT_STATUS_KEY_LEADER]
			if !ok || v == RAFT_EMPTY_ADDR {
				noLeaderCopysetPeers[groupId] = strings.Split(statMap[RAFT_STATUS_KEY_PEERS], " ")
			}
			// not leader and not follower - but "transferring" or "candidate"
		} else if state == RAFT_STATUS_STATE_TRANSFERRING || state == RAFT_STATUS_STATE_CANDIDATE {
			cs.recordCopyset(COPYSET_CHECK_NO_LEADER, groupId)
			cs.recordCopysetStat(int32(cobrautil.COPYSET_WARN), groupId)
			healthy = false
		} else {
			// other state: ERROR,UNINITIALIZED,SHUTTING和SHUTDOWN
			key := fmt.Sprintf("state %s", state)
			cs.recordCopyset(key, groupId)
			healthy = false
		}
	}

	health := cs.checkCopysetsNoLeader(addr, &noLeaderCopysetPeers, cmd)
	if !health {
		healthy = false
	}
	return healthy
}

func (cs *Copyset) checkCopysetsWithMds(cmd *cobra.Command) (bool, int, error) {
	// get copysets in cluster
	csInfos, err := copyset.GetCopySetsInCluster(cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return false, 0, fmt.Errorf("GetCopySetsInCluster failed, %v", err)
	}
	infos := []CopySetInfo{}
	for _, csInfo := range csInfos {
		info := CopySetInfo{}
		info.CopysetId = csInfo.GetCopysetId()
		info.LogicalPoolId = csInfo.GetLogicalPoolId()
		infos = append(infos, info)
	}

	// check copyset number
	if len(csInfos) != cs.copyset[COPYSET_TOTAL].Cardinality() {
		return false, len(csInfos), fmt.Errorf("Copyset numbers in chunkservers not consistent with mds,"+
			"please check! copysets on chunkserver: %d; copysets in mds: %d",
			cs.copyset[COPYSET_TOTAL].Cardinality(), len(csInfos))
	}

	// check copyset groupId difference
	groupIdsInMds := set.NewSet[string]()
	for _, info := range infos {
		groupIdsInMds.Add(strconv.FormatUint(getGroupId(info.LogicalPoolId, info.CopysetId), 10))
	}
	notInChunkServer := groupIdsInMds.Difference(cs.copyset[COPYSET_TOTAL])
	if notInChunkServer.Cardinality() != 0 {
		return false, len(csInfos), fmt.Errorf("some copysets in mds but not in chunkserver: %v", notInChunkServer)
	}
	notInMds := cs.copyset[COPYSET_TOTAL].Difference(groupIdsInMds)
	if notInMds.Cardinality() != 0 {
		return false, len(csInfos), fmt.Errorf("some copysets in chunkserver but not in mds: %v", notInMds)
	}

	// check copyset data consistency scanned result
	count := 0
	for _, info := range infos {
		if info.LastScanSec == 0 || (info.LastScanSec != 0 && info.LastScanConsistent) {
			continue
		}
		groupId := strconv.FormatUint(getGroupId(info.LogicalPoolId, info.CopysetId), 10)
		cs.recordCopyset(COPYSET_CHECK_INCONSISTENT, groupId)
		count++
	}
	if count > 0 {
		return false, len(csInfos), fmt.Errorf("There are %d inconsistent copyset", count)
	}
	return true, len(csInfos), nil
}

func (cs *Copyset) checkCopysetsInCluster(cmd *cobra.Command) (bool, error) {
	healthy := true
	// 1. get chunkservers in cluster
	chunkservers, err := chunkserver.GetChunkServerInCluster(cmd)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return false, fmt.Errorf("GetChunkServerInCluster failed %v", err)
	}

	// 2. get copyset raft status
	csAddrs := []string{}
	addr2Id := make(map[string]uint32)
	for _, cs := range chunkservers {
		addr := fmt.Sprintf("%s:%d", cs.GetHostIp(), cs.GetPort())
		csAddrs = append(csAddrs, addr)
		addr2Id[addr] = *cs.ChunkServerID
	}
	status, err2 := GetCopysetRaftStatus(csAddrs, cmd)
	if err2 != nil {
		return false, err2
	}
	for k, v := range status {
		cs.updateChunkServerCopysets(k, v)
	}

	// 3. check copyset status on chunkserver
	// k-> chunkserver addr
	// v-> []map[string]string
	for k, v := range status {
		if !cs.checkCopysetsOnChunkServer(k, v, addr2Id, cmd) {
			healthy = false
		}
	}

	return healthy, nil
}

func (cs *Copyset) getCopysetTotalNum() uint32 {
	if total, ok := cs.copyset[COPYSET_TOTAL]; ok {
		return uint32(total.Cardinality())
	}
	return 0
}

func (cs *Copyset) getCopysetUnhealthyNum() uint32 {
	var number uint32
	for k, v := range cs.copyset {
		if k != COPYSET_TOTAL {
			number += uint32(v.Cardinality())
		}
	}
	return number
}

func MaxUint64(first, second uint64) uint64 {
	if first < second {
		return second
	}
	return first
}

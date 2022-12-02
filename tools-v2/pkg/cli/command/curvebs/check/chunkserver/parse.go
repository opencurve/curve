package chunkserver

import (
	"regexp"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"golang.org/x/exp/slices"
)

const (
	GROUPID         = "groupId"
	GROUPID_PATTERN = `\[(\d+)\]`
	PEERID          = "peer_id"
	PEER_ID_PATTERN = `peer_id: ([\s\S]*)`
	STATE           = "state"
	STATE_PATTERN   = `state: ([A-Z]*)`
	PEERS           = "peers"
	PEERS_PATTERN   = `peers: ([\s\S]*)`
	LEADER_PATTERN  = `leader: ([\s\S]*)`

	LEADER              = "leader"
	LAST_LOG_ID         = "last_log_id"
	LAST_LOG_ID_PATTERN = `last_log_id:\s*\(([\s\S]*)\)`

	REPLICATOR                    = "replicator"
	REPLICATOR_PATTERN            = `replicator_\d+@([\s\S]*)`
	REPLICATOR_ADDR_PATTERN       = `([\s\S]*):0`
	REPLICATOR_NEXT_INDEX_PATTERN = `next_index=(\d*)\s`
	REPLICATOR_FLYING_PATTERN     = `flying_append_entries_size=(\d*)\s`
	REPLICATOR_SNAPSHOT_PATTERN   = `snapshot`
	REPLICATOR_IDLE_PATTERN       = `\s([a-z]+)\s`
	REPLICATOR_HC_PATTERN         = `hc=(\d*)\s`
	REPLICATOR_AC_PATTERN         = `ac=(\d*)\s`
	REPLICATOR_IC_PATTERN         = `ic=(\d*)`

	SNAPSHOT = "installing snapshot"
)

/*
get and parse chunkserver raft status data:

	groupId: 复制组的groupId
	peer_id: 10.182.26.45:8210:0格式的peer id
	state: 节点的状态，LEADER,FOLLOWER,CANDIDATE等等
	peers: 配置组里的成员，通过空格分隔
	last_log_id: 最后一个log entry的index
	replicator: state为LEADER时才存在这个key，指向复制组leader
	replicator_1: 第一个follower的复制状态,value如下：
	next_index=6349842  flying_append_entries_size=0 idle hc=1234 ac=123 ic=0
	next_index为下一个要发送给该follower的index
*/
func (cksCmd *ChunkServerCmd) GetAndParseCpsData(csAddr string) (*cmderror.CmdError, []map[string]string) {
	metric := basecmd.NewMetric([]string{csAddr}, "/raft_stat", cksCmd.timeout)
	result, err := basecmd.QueryMetric(metric)
	var raft_stat_info []map[string]string
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		index := slices.Index(cksCmd.abnormalHosts, csAddr)
		if index == -1 {
			cksCmd.abnormalHosts = append(cksCmd.abnormalHosts, csAddr)
		}
		return cmderror.ErrBsGetRaftStatus(statuscode.CsHealthStatus_CsNotOnline, csAddr), raft_stat_info
	}
	lines := strings.Split(result, "\n")
	length := len(lines)
	index := 0
	for index < length && len(lines[index]) > 0 && strings.HasPrefix(lines[index], "[") {
		groupInfoMap := make(map[string]string)
		gidRegexp := regexp.MustCompile(GROUPID_PATTERN)
		res := gidRegexp.FindStringSubmatch(lines[index])
		if len(res) != 2 {
			parseErr := cmderror.ErrCopysetGapKey()
			parseErr.Format(lines[index])
			return parseErr, raft_stat_info
		}
		groupInfoMap[GROUPID] = res[1]
		nextIndex := index + 1
		peer_idFlag := false
		stateFlag := false
		peersFlag := false
		Isleader := false
		leaderFlag := false
		lastLogIdFlag := false
		replicatorCnt := 0
		for nextIndex < length && len(lines[nextIndex]) > 0 && lines[nextIndex][0] != '[' {
			line := lines[nextIndex]
			if !peer_idFlag && strings.HasPrefix(line, PEERID) {
				r := regexp.MustCompile(PEER_ID_PATTERN)
				res := r.FindStringSubmatch(line)
				if len(res) != 2 {
					parseErr := cmderror.ErrParseRaftState()
					parseErr.Format(PEERID, line)
					return parseErr, raft_stat_info
				}
				peer_idFlag = true
				groupInfoMap[PEERID] = res[1]
			} else if !stateFlag && strings.HasPrefix(line, STATE) {
				stateRegexp := regexp.MustCompile(STATE_PATTERN)
				res := stateRegexp.FindStringSubmatch(line)
				if len(res) != 2 {
					parseErr := cmderror.ErrParseRaftState()
					parseErr.Format(STATE, line)
					return parseErr, raft_stat_info
				}
				if res[1] == "LEADER" {
					Isleader = true
				}
				stateFlag = true
				groupInfoMap[STATE] = res[1]
			} else if !peersFlag && strings.HasPrefix(line, PEERS) {
				r := regexp.MustCompile(PEERS_PATTERN)
				res := r.FindStringSubmatch(line)
				if len(res) != 2 {
					parseErr := cmderror.ErrParseRaftState()
					parseErr.Format(PEERS, line)
					return parseErr, raft_stat_info
				}
				peersFlag = true
				groupInfoMap[PEERS] = res[1]
			} else if !leaderFlag && strings.HasPrefix(line, LEADER) {
				r := regexp.MustCompile(LEADER_PATTERN)
				res := r.FindStringSubmatch(line)
				if len(res) != 2 {
					parseErr := cmderror.ErrParseRaftState()
					parseErr.Format("leader", line)
					return parseErr, raft_stat_info
				}
				leaderFlag = true
				groupInfoMap[LEADER] = res[1]
			} else if !lastLogIdFlag && strings.HasPrefix(line, LAST_LOG_ID) {
				r := regexp.MustCompile(LAST_LOG_ID_PATTERN)
				res := r.FindStringSubmatch(line)
				if len(res) != 2 {
					parseErr := cmderror.ErrParseRaftState()
					parseErr.Format(LAST_LOG_ID, line)
					return parseErr, raft_stat_info
				}
				lastLogIdFlag = true
				groupInfoMap[LAST_LOG_ID] = res[1]
			} else if Isleader && replicatorCnt < 2 && strings.HasPrefix(line, REPLICATOR) {
				r := regexp.MustCompile(REPLICATOR_PATTERN)
				res := r.FindStringSubmatch(line)
				if len(res) != 2 {
					parseErr := cmderror.ErrParseRaftState()
					parseErr.Format(REPLICATOR, line)
					return parseErr, raft_stat_info
				}
				replicatorCnt++
				if replicatorCnt == 1 {
					groupInfoMap[REPLICATOR+"1"] = res[1]
				} else if replicatorCnt == 2 {
					groupInfoMap[REPLICATOR+"2"] = res[1]
				}

				ParseReplicatorData(res[1])
			}
			nextIndex++
		}
		raft_stat_info = append(raft_stat_info, groupInfoMap)
		index = nextIndex
	}
	return cmderror.ErrSuccess(), raft_stat_info
}

func ParseLastLogId(lastlogid string) string {
	pattern := `index=(\d*),term=(\d*)`
	r := regexp.MustCompile(pattern)
	res := r.FindStringSubmatch(lastlogid)
	if len(res) < 3 {
		return ""
	}
	return res[1]
}
func ParseReplicatorData(rep string) (string, string, string) {
	r := regexp.MustCompile(REPLICATOR_NEXT_INDEX_PATTERN)
	res := r.FindStringSubmatch(rep)
	nextIndex := ""
	if len(res) == 2 {
		nextIndex = res[1]
	}
	r = regexp.MustCompile(REPLICATOR_FLYING_PATTERN)
	res = r.FindStringSubmatch(rep)
	flying := ""
	if len(res) == 2 {
		flying = res[1]
	}
	snapshot := ""
	r = regexp.MustCompile(REPLICATOR_SNAPSHOT_PATTERN)
	res = r.FindStringSubmatch(rep)
	if len(res) != 0 {
		snapshot = "snapshot"
	}
	return nextIndex, flying, snapshot
}
